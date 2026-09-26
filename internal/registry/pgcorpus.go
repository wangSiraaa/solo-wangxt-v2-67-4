package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"protocompat/internal/corpus"
)

// --- content-addressed samples -----------------------------------------

func (s *PGStore) PutSample(ctx context.Context, pkg string, rec CorpusSampleRecord) (bool, error) {
	var expectation []byte
	if rec.Expectation != nil {
		b, err := json.Marshal(rec.Expectation)
		if err != nil {
			return false, err
		}
		expectation = b
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	pkgID, err := s.ensurePackage(ctx, tx, pkg)
	if err != nil {
		return false, err
	}
	var insertedID int64
	err = tx.QueryRowContext(ctx, `
		INSERT INTO corpus_samples (package_id, digest, name, message, encoding, data, expectation)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (package_id, digest) DO NOTHING
		RETURNING id`,
		pkgID, rec.Digest, rec.Name, rec.Message, rec.Encoding, rec.Data, expectation).Scan(&insertedID)
	switch {
	case err == nil:
		return true, tx.Commit()
	case errors.Is(err, sql.ErrNoRows):
		return false, tx.Commit() // identical content already present
	default:
		return false, fmt.Errorf("put corpus sample: %w", err)
	}
}

func (s *PGStore) GetSample(ctx context.Context, pkg, digest string) (*CorpusSampleRecord, error) {
	var rec CorpusSampleRecord
	var expectation []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT p.name, cs.digest, cs.name, cs.message, cs.encoding, cs.data, cs.expectation, cs.created_at
		FROM corpus_samples cs JOIN packages p ON p.id = cs.package_id
		WHERE p.name = $1 AND cs.digest = $2`, pkg, digest).
		Scan(&pkg, &rec.Digest, &rec.Name, &rec.Message, &rec.Encoding, &rec.Data, &expectation, &rec.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if expectation != nil {
		rec.Expectation = &corpus.Expectation{}
		if err := json.Unmarshal(expectation, rec.Expectation); err != nil {
			return nil, fmt.Errorf("decode expectation: %w", err)
		}
	}
	return &rec, nil
}

// --- draft / sealed sets ------------------------------------------------

func (s *PGStore) CreateDraft(ctx context.Context, pkg, name string, baseVersion int, note string, add, remove []string) (*CorpusSet, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()
	pkgID, err := s.ensurePackage(ctx, tx, pkg)
	if err != nil {
		return nil, false, err
	}

	// One open draft per corpus at a time.
	var existingID int64
	var existingStatus string
	err = tx.QueryRowContext(ctx, `
		SELECT id, status FROM corpus_sets
		WHERE package_id = $1 AND corpus = $2 AND version = (
			SELECT max(version) FROM corpus_sets WHERE package_id = $1 AND corpus = $2)`,
		pkgID, name).Scan(&existingID, &existingStatus)
	draftExists := false
	switch {
	case err == nil:
		if existingStatus == CorpusDraft {
			draftExists = true
		}
	case errors.Is(err, sql.ErrNoRows):
		// first version
	default:
		return nil, false, err
	}
	if draftExists {
		set, gerr := s.getSetTx(ctx, tx, pkgID, existingID)
		if gerr != nil {
			return nil, false, gerr
		}
		return set, false, ErrDraftExists
	}

	// Base membership.
	var baseSetID int64
	membership := map[int64]bool{}
	var ordered []int64
	if baseVersion > 0 {
		err = tx.QueryRowContext(ctx, `
			SELECT id, status FROM corpus_sets WHERE package_id = $1 AND corpus = $2 AND version = $3`,
			pkgID, name, baseVersion).Scan(&baseSetID, &existingStatus)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, ErrNotFound
		}
		if err != nil {
			return nil, false, err
		}
		if existingStatus == CorpusDeleted {
			return nil, false, ErrNotFound
		}
		if existingStatus != CorpusSealed {
			return nil, false, fmt.Errorf("base version %d must be sealed before deriving a new version", baseVersion)
		}
		ordered, err = s.memberIDsTx(ctx, tx, baseSetID)
		if err != nil {
			return nil, false, err
		}
		for _, id := range ordered {
			membership[id] = true
		}
	}

	if err := applyChangesTx(ctx, tx, pkgID, membership, &ordered, add, remove); err != nil {
		return nil, false, err
	}
	if baseVersion > 0 && len(add) == 0 && len(remove) == 0 {
		return nil, false, ErrNoChange
	}

	var nextVersion int
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(max(version), 0) + 1 FROM corpus_sets WHERE package_id = $1 AND corpus = $2`,
		pkgID, name).Scan(&nextVersion); err != nil {
		return nil, false, err
	}
	var setID int64
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO corpus_sets (package_id, corpus, version, status, note)
		VALUES ($1, $2, $3, 'draft', $4) RETURNING id`,
		pkgID, name, nextVersion, note).Scan(&setID); err != nil {
		return nil, false, fmt.Errorf("insert corpus set: %w", err)
	}
	for pos, sampleID := range ordered {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO corpus_entries (set_id, sample_id, position) VALUES ($1, $2, $3)`,
			setID, sampleID, pos); err != nil {
			return nil, false, fmt.Errorf("insert corpus entry: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	set, err := s.getSet(ctx, pkg, setID)
	return set, true, err
}

func (s *PGStore) UpdateDraft(ctx context.Context, pkg, name, note string, add, remove []string) (*CorpusSet, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	pkgID, err := s.ensurePackage(ctx, tx, pkg)
	if err != nil {
		return nil, err
	}
	var setID int64
	var status string
	err = tx.QueryRowContext(ctx, `
		SELECT id, status FROM corpus_sets
		WHERE package_id = $1 AND corpus = $2 AND version = (
			SELECT max(version) FROM corpus_sets WHERE package_id = $1 AND corpus = $2 AND status <> 'deleted')`,
		pkgID, name).Scan(&setID, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoDraft
	}
	if err != nil {
		return nil, err
	}
	if status == CorpusSealed {
		return nil, ErrSealed
	}
	if status == CorpusDeleted {
		return nil, ErrNoDraft
	}

	ordered, err := s.memberIDsTx(ctx, tx, setID)
	if err != nil {
		return nil, err
	}
	membership := map[int64]bool{}
	for _, id := range ordered {
		membership[id] = true
	}
	if err := applyChangesTx(ctx, tx, pkgID, membership, &ordered, add, remove); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM corpus_entries WHERE set_id = $1`, setID); err != nil {
		return nil, err
	}
	for pos, sampleID := range ordered {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO corpus_entries (set_id, sample_id, position) VALUES ($1, $2, $3)`,
			setID, sampleID, pos); err != nil {
			return nil, fmt.Errorf("reinsert corpus entry: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE corpus_sets SET note = $2 WHERE id = $1`, setID, note); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.getSet(ctx, pkg, setID)
}

// applyChangesTx resolves digest add/remove lists against package samples
// and mutates the ordered membership. Removals of absent members and
// additions of present members are tolerated (set semantics); the caller
// detects "no effective change".
func applyChangesTx(ctx context.Context, tx *sql.Tx, pkgID int64, membership map[int64]bool, ordered *[]int64, add, remove []string) error {
	removeDigests := map[string]bool{}
	for _, d := range remove {
		removeDigests[d] = true
	}
	for _, d := range add {
		var id int64
		err := tx.QueryRowContext(ctx, `SELECT id FROM corpus_samples WHERE package_id = $1 AND digest = $2`,
			pkgID, d).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("sample %s: %w", d, ErrNotFound)
		}
		if err != nil {
			return err
		}
		if !membership[id] {
			membership[id] = true
			*ordered = append(*ordered, id)
		}
	}
	if len(removeDigests) > 0 {
		filtered := (*ordered)[:0]
		for _, id := range *ordered {
			var digest string
			if err := tx.QueryRowContext(ctx, `SELECT digest FROM corpus_samples WHERE id = $1`, id).Scan(&digest); err != nil {
				return err
			}
			if removeDigests[digest] {
				delete(membership, id)
				continue
			}
			filtered = append(filtered, id)
		}
		*ordered = filtered
	}
	return nil
}

func (s *PGStore) memberIDsTx(ctx context.Context, tx *sql.Tx, setID int64) ([]int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT sample_id FROM corpus_entries WHERE set_id = $1 ORDER BY position`, setID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *PGStore) SealCorpus(ctx context.Context, pkg, name string) (*CorpusSet, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	pkgID, err := s.lookupPackage(ctx, tx, pkg)
	if err != nil {
		return nil, err
	}
	var setID int64
	var status string
	err = tx.QueryRowContext(ctx, `
		SELECT id, status FROM corpus_sets
		WHERE package_id = $1 AND corpus = $2 AND version = (
			SELECT max(version) FROM corpus_sets WHERE package_id = $1 AND corpus = $2 AND status <> 'deleted')`,
		pkgID, name).Scan(&setID, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoDraft
	}
	if err != nil {
		return nil, err
	}
	if status == CorpusSealed {
		return nil, ErrSealed
	}
	if status == CorpusDeleted {
		return nil, ErrNoDraft
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM corpus_entries WHERE set_id = $1`, setID).Scan(&count); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, fmt.Errorf("cannot seal an empty corpus")
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE corpus_sets SET status = 'sealed', sealed_at = now() WHERE id = $1`, setID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.getSet(ctx, pkg, setID)
}

func (s *PGStore) DeleteDraft(ctx context.Context, pkg, name string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkgID, err := s.lookupPackage(ctx, tx, pkg)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE corpus_sets SET status = 'deleted'
		WHERE package_id = $1 AND corpus = $2
		  AND version = (SELECT max(version) FROM corpus_sets cs2
		                 WHERE cs2.package_id = $1 AND cs2.corpus = $2)
		  AND status = 'draft'`, pkgID, name)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNoDraft
	}
	return tx.Commit()
}

func (s *PGStore) GetCorpusSet(ctx context.Context, pkg, name string, version int) (*CorpusSet, error) {
	var setID int64
	err := s.db.QueryRowContext(ctx, `
		SELECT cs.id FROM corpus_sets cs JOIN packages p ON p.id = cs.package_id
		WHERE p.name = $1 AND cs.corpus = $2 AND cs.version = $3`,
		pkg, name, version).Scan(&setID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return s.getSet(ctx, pkg, setID)
}

func (s *PGStore) GetDraft(ctx context.Context, pkg, name string) (*CorpusSet, error) {
	var setID int64
	err := s.db.QueryRowContext(ctx, `
		SELECT cs.id FROM corpus_sets cs JOIN packages p ON p.id = cs.package_id
		WHERE p.name = $1 AND cs.corpus = $2
		ORDER BY cs.version DESC LIMIT 1`, pkg, name).Scan(&setID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	set, err := s.getSet(ctx, pkg, setID)
	if err != nil {
		return nil, err
	}
	if set.Status != CorpusDraft {
		return nil, ErrNotFound
	}
	return set, nil
}

func (s *PGStore) LatestCorpusVersion(ctx context.Context, pkg, name string) (*CorpusSet, error) {
	var setID int64
	err := s.db.QueryRowContext(ctx, `
		SELECT cs.id FROM corpus_sets cs JOIN packages p ON p.id = cs.package_id
		WHERE p.name = $1 AND cs.corpus = $2 AND cs.status <> 'deleted'
		ORDER BY cs.version DESC LIMIT 1`, pkg, name).Scan(&setID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return s.getSet(ctx, pkg, setID)
}

// ListCorpora returns the latest non-deleted version of every corpus in
// the package (a tombstoned draft is skipped, so the corpus falls back to
// its most recent sealed version).
func (s *PGStore) ListCorpora(ctx context.Context, pkg string) ([]CorpusSet, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT cs.id
		FROM corpus_sets cs
		JOIN packages p ON p.id = cs.package_id
		WHERE p.name = $1 AND cs.status <> 'deleted'
		  AND cs.version = (
		      SELECT max(cs2.version) FROM corpus_sets cs2
		      WHERE cs2.package_id = cs.package_id
		        AND cs2.corpus = cs.corpus
		        AND cs2.status <> 'deleted')
		ORDER BY cs.corpus`, pkg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]CorpusSet, 0, len(ids))
	for _, id := range ids {
		set, err := s.getSet(ctx, pkg, id)
		if err != nil {
			return nil, err
		}
		out = append(out, *set)
	}
	return out, nil
}

func (s *PGStore) lookupPackage(ctx context.Context, tx *sql.Tx, name string) (int64, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT id FROM packages WHERE name = $1`, name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return id, err
}

func (s *PGStore) getSet(ctx context.Context, pkg string, setID int64) (*CorpusSet, error) {
	var set CorpusSet
	var sealed sql.NullTime
	err := s.db.QueryRowContext(ctx, `
		SELECT p.name, cs.corpus, cs.version, cs.status, cs.note, cs.created_at, cs.sealed_at
		FROM corpus_sets cs JOIN packages p ON p.id = cs.package_id
		WHERE cs.id = $1`, setID).
		Scan(&set.Package, &set.Corpus, &set.Version, &set.Status, &set.Note, &set.CreatedAt, &sealed)
	if err != nil {
		return nil, err
	}
	set.ID = setID
	if sealed.Valid {
		t := sealed.Time
		set.SealedAt = &t
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT cs2.digest FROM corpus_entries e
		JOIN corpus_samples cs2 ON cs2.id = e.sample_id
		WHERE e.set_id = $1 ORDER BY e.position`, setID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		set.Digests = append(set.Digests, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	set.SampleCount = len(set.Digests)
	_ = pkg
	return &set, nil
}

func (s *PGStore) getSetTx(ctx context.Context, tx *sql.Tx, pkgID, setID int64) (*CorpusSet, error) {
	var set CorpusSet
	var sealed sql.NullTime
	err := tx.QueryRowContext(ctx, `
		SELECT p.name, cs.corpus, cs.version, cs.status, cs.note, cs.created_at, cs.sealed_at
		FROM corpus_sets cs JOIN packages p ON p.id = cs.package_id
		WHERE cs.id = $1`, setID).
		Scan(&set.Package, &set.Corpus, &set.Version, &set.Status, &set.Note, &set.CreatedAt, &sealed)
	if err != nil {
		return nil, err
	}
	set.ID = setID
	if sealed.Valid {
		t := sealed.Time
		set.SealedAt = &t
	}
	set.Digests, err = s.memberDigestsTx(ctx, tx, setID)
	if err != nil {
		return nil, err
	}
	set.SampleCount = len(set.Digests)
	_ = pkgID
	return &set, nil
}

func (s *PGStore) memberDigestsTx(ctx context.Context, tx *sql.Tx, setID int64) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT cs.digest FROM corpus_entries e
		JOIN corpus_samples cs ON cs.id = e.sample_id
		WHERE e.set_id = $1 ORDER BY e.position`, setID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// --- replays ------------------------------------------------------------

func (s *PGStore) CreateReplay(ctx context.Context, r Replay, items []ReplayItem) (*Replay, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	pkgID, err := s.ensurePackage(ctx, tx, r.Package)
	if err != nil {
		return nil, err
	}
	// Replays target sealed sets only.
	var setStatus string
	err = tx.QueryRowContext(ctx, `SELECT status FROM corpus_sets WHERE id = $1`, r.SetID).Scan(&setStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if setStatus != CorpusSealed {
		return nil, fmt.Errorf("replays can only target a sealed corpus set")
	}

	var replayID int64
	err = tx.QueryRowContext(ctx, `
		INSERT INTO replays (package_id, replay_key, corpus, corpus_version, set_id, schema_version, status, total)
		VALUES ($1, $2, $3, $4, $5, $6, 'running', $7)
		RETURNING id`,
		pkgID, r.Key, r.Corpus, r.CorpusVersion, r.SetID, r.SchemaVersion, len(items)).Scan(&replayID)
	if uniqueViolation(err) {
		existing, gerr := s.getReplayByKey(ctx, r.Package, r.Key)
		if gerr != nil {
			return nil, gerr
		}
		return existing, &ReplayExistenceError{Replay: existing}
	}
	if err != nil {
		return nil, fmt.Errorf("insert replay: %w", err)
	}
	for i, it := range items {
		sampleBody, mErr := json.Marshal(it.Sample)
		if mErr != nil {
			return nil, mErr
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO replay_items (replay_id, position, digest, sample, status)
			VALUES ($1, $2, $3, $4, 'pending')`,
			replayID, i, it.Digest, sampleBody); err != nil {
			return nil, fmt.Errorf("insert replay item: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.GetReplay(ctx, r.Package, replayID)
}

func uniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "duplicate key")
}

func (s *PGStore) ClaimNextItem(ctx context.Context, replayID int64, claimBefore time.Time) (*ReplayItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Prefer fresh pending work; otherwise reclaim an expired lease from a
	// crashed worker. ORDER BY position keeps the run deterministic.
	var position int64
	err = tx.QueryRowContext(ctx, `
		UPDATE replay_items SET status = 'claimed', attempt = attempt + 1,
		                        claimed_at = now(), updated_at = now()
		WHERE (replay_id, position) IN (
		    SELECT replay_id, position FROM replay_items
		    WHERE replay_id = $1 AND (
		          status = 'pending'
		          OR (status = 'claimed' AND claimed_at < $2))
		    ORDER BY position LIMIT 1
		    FOR UPDATE SKIP LOCKED)
		RETURNING position`, replayID, claimBefore).Scan(&position)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim item: %w", err)
	}
	item, err := s.getReplayItemTx(ctx, tx, replayID, position)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return item, nil
}

func (s *PGStore) CompleteItem(ctx context.Context, replayID int64, position int, result corpus.ItemResult) error {
	body, err := json.Marshal(result)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status string
	err = tx.QueryRowContext(ctx, `
		SELECT status FROM replay_items WHERE replay_id = $1 AND position = $2 FOR UPDATE`,
		replayID, position).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status == ItemCompleted {
		// Same input key already produced a result: never overwrite.
		return ErrItemAlreadyComplete
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE replay_items
		SET status = 'completed', result = $3, updated_at = now()
		WHERE replay_id = $1 AND position = $2`, replayID, position, body); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE replays SET completed = (
		    SELECT count(*) FROM replay_items WHERE replay_id = $1 AND status = 'completed'),
		    updated_at = now()
		WHERE id = $1`, replayID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *PGStore) CompleteReplay(ctx context.Context, replayID int64, status string, summary corpus.Summary) error {
	body, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE replays SET status = $2, summary = $3, updated_at = now()
		WHERE id = $1 AND status IN ('running', 'failed')`, replayID, status, body)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PGStore) GetReplay(ctx context.Context, pkg string, id int64) (*Replay, error) {
	return s.queryReplay(ctx, `
		SELECT r.id, p.name, r.replay_key, r.corpus, r.corpus_version, r.set_id,
		       r.schema_version, r.status, r.total, r.completed, r.summary,
		       r.created_at, r.updated_at
		FROM replays r JOIN packages p ON p.id = r.package_id
		WHERE p.name = $1 AND r.id = $2`, pkg, id)
}

func (s *PGStore) getReplayByKey(ctx context.Context, pkg, key string) (*Replay, error) {
	return s.queryReplay(ctx, `
		SELECT r.id, p.name, r.replay_key, r.corpus, r.corpus_version, r.set_id,
		       r.schema_version, r.status, r.total, r.completed, r.summary,
		       r.created_at, r.updated_at
		FROM replays r JOIN packages p ON p.id = r.package_id
		WHERE p.name = $1 AND r.replay_key = $2`, pkg, key)
}

func (s *PGStore) GetReplayByKey(ctx context.Context, pkg, key string) (*Replay, error) {
	r, err := s.getReplayByKey(ctx, pkg, key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return r, err
}

func (s *PGStore) queryReplay(ctx context.Context, query, pkg string, arg any) (*Replay, error) {
	var r Replay
	var summary []byte
	err := s.db.QueryRowContext(ctx, query, pkg, arg).Scan(
		&r.ID, &r.Package, &r.Key, &r.Corpus, &r.CorpusVersion, &r.SetID,
		&r.SchemaVersion, &r.Status, &r.Total, &r.Completed, &summary,
		&r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if summary != nil {
		r.Summary = &corpus.Summary{}
		if err := json.Unmarshal(summary, r.Summary); err != nil {
			return nil, fmt.Errorf("decode summary: %w", err)
		}
	}
	return &r, nil
}

func (s *PGStore) ListReplays(ctx context.Context, pkg, corpusName string) ([]Replay, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if corpusName == "" {
		rows, err = s.db.QueryContext(ctx, `
			SELECT r.id, p.name, r.replay_key, r.corpus, r.corpus_version, r.set_id,
			       r.schema_version, r.status, r.total, r.completed, r.summary,
			       r.created_at, r.updated_at
			FROM replays r JOIN packages p ON p.id = r.package_id
			WHERE p.name = $1 ORDER BY r.id`, pkg)
	} else {
		rows, err = s.db.QueryContext(ctx, `
			SELECT r.id, p.name, r.replay_key, r.corpus, r.corpus_version, r.set_id,
			       r.schema_version, r.status, r.total, r.completed, r.summary,
			       r.created_at, r.updated_at
			FROM replays r JOIN packages p ON p.id = r.package_id
			WHERE p.name = $1 AND r.corpus = $2 ORDER BY r.id`, pkg, corpusName)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Replay
	for rows.Next() {
		var r Replay
		var summary []byte
		if err := rows.Scan(
			&r.ID, &r.Package, &r.Key, &r.Corpus, &r.CorpusVersion, &r.SetID,
			&r.SchemaVersion, &r.Status, &r.Total, &r.Completed, &summary,
			&r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		if summary != nil {
			r.Summary = &corpus.Summary{}
			if err := json.Unmarshal(summary, r.Summary); err != nil {
				return nil, err
			}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PGStore) ListReplayItems(ctx context.Context, replayID int64) ([]ReplayItem, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT position, digest, sample, status, attempt, result, claimed_at, updated_at
		FROM replay_items WHERE replay_id = $1 ORDER BY position`, replayID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReplayItem
	for rows.Next() {
		var it ReplayItem
		var sampleBody, resultBody []byte
		var claimed sql.NullTime
		if err := rows.Scan(&it.Position, &it.Digest, &sampleBody, &it.Status, &it.Attempt,
			&resultBody, &claimed, &it.UpdatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(sampleBody, &it.Sample); err != nil {
			return nil, err
		}
		if resultBody != nil {
			it.Result = &corpus.ItemResult{}
			if err := json.Unmarshal(resultBody, it.Result); err != nil {
				return nil, err
			}
		}
		if claimed.Valid {
			t := claimed.Time
			it.ClaimedAt = &t
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

func (s *PGStore) getReplayItemTx(ctx context.Context, tx *sql.Tx, replayID, position int64) (*ReplayItem, error) {
	var it ReplayItem
	var sampleBody []byte
	var claimed sql.NullTime
	err := tx.QueryRowContext(ctx, `
		SELECT position, digest, sample, status, attempt, claimed_at, updated_at
		FROM replay_items WHERE replay_id = $1 AND position = $2`,
		replayID, position).
		Scan(&it.Position, &it.Digest, &sampleBody, &it.Status, &it.Attempt, &claimed, &it.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(sampleBody, &it.Sample); err != nil {
		return nil, err
	}
	if claimed.Valid {
		t := claimed.Time
		it.ClaimedAt = &t
	}
	return &it, nil
}
