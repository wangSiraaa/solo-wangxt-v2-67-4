package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"protocompat/internal/replay"
)

func scanCorpusSet(row interface {
	Scan(dest ...any) error
}, set *CorpusSet, pkgName, corpusName *string) error {
	var status string
	var base sql.NullInt64
	var sealed sql.NullTime
	if err := row.Scan(&set.ID, pkgName, corpusName, &set.Version, &status, &base, &set.CreatedAt, &sealed); err != nil {
		return err
	}
	set.Status = status
	if base.Valid {
		v := int(base.Int64)
		set.Base = &v
	}
	if sealed.Valid {
		t := sealed.Time
		set.SealedAt = &t
	}
	return nil
}

const selectCorpusSet = `
SELECT cs.id, p.name, cs.name, cs.version, cs.status, cs.base_id, cs.created_at, cs.sealed_at
FROM corpus_sets cs JOIN packages p ON p.id = cs.package_id`

func (s *PGStore) corpusPackageID(ctx context.Context, tx *sql.Tx, pkg string) (int64, error) {
	return s.ensurePackage(ctx, tx, pkg)
}

type preparedCorpusSample struct {
	input CorpusSampleInput
	hash  []byte
}

func prepareCorpusMutation(mutation CorpusMutation) ([]preparedCorpusSample, error) {
	seen := map[string]bool{}
	prep := make([]preparedCorpusSample, 0, len(mutation.AddOrUpdate))
	for _, in := range mutation.AddOrUpdate {
		normalized, fp, err := normalizeCorpusInput(in)
		if err != nil {
			return nil, err
		}
		if seen[normalized.Key] {
			return nil, fmt.Errorf("%w: duplicate sample key %s in request", ErrCorpusConflict, normalized.Key)
		}
		seen[normalized.Key] = true
		prep = append(prep, preparedCorpusSample{input: normalized, hash: fp})
	}
	for _, key := range mutation.DeleteKeys {
		if seen[key] {
			return nil, fmt.Errorf("%w: key %s is both added and deleted", ErrCorpusConflict, key)
		}
	}
	return prep, nil
}

func (s *PGStore) upsertCorpusSampleTx(ctx context.Context, tx *sql.Tx, pkgID int64, p preparedCorpusSample) (int64, error) {
	expected, err := json.Marshal(p.input.ExpectedResult)
	if err != nil {
		return 0, err
	}
	name := p.input.Name
	if name == "" {
		name = p.input.Key
	}
	var id int64
	err = tx.QueryRowContext(ctx, `
INSERT INTO corpus_samples
  (package_id, fingerprint, name, message_name, encoding, data, expected_result, content_summary)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (package_id, fingerprint) DO NOTHING
RETURNING id`,
		pkgID, p.hash, name, p.input.Message, p.input.Encoding, p.input.Data, expected, p.input.ContentSummary).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("upsert corpus sample: %w", err)
	}
	err = tx.QueryRowContext(ctx, `SELECT id FROM corpus_samples WHERE package_id = $1 AND fingerprint = $2`, pkgID, p.hash).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *PGStore) applyCorpusMutationTx(ctx context.Context, tx *sql.Tx, pkgID, setID int64, pkg string, mutation CorpusMutation) error {
	prep, err := prepareCorpusMutation(mutation)
	if err != nil {
		return err
	}
	for _, p := range prep {
		sampleID, err := s.upsertCorpusSampleTx(ctx, tx, pkgID, p)
		if err != nil {
			return err
		}
		name := p.input.Name
		if name == "" {
			name = p.input.Key
		}
		var existingKey string
		keyLookupErr := tx.QueryRowContext(ctx, `
SELECT sample_key FROM corpus_set_samples WHERE corpus_set_id = $1 AND sample_id = $2`,
			setID, sampleID).Scan(&existingKey)
		switch {
		case keyLookupErr == nil && existingKey != p.input.Key:
			return fmt.Errorf("%w: identical content is already stored with key %s", ErrCorpusConflict, existingKey)
		case keyLookupErr != nil && !errors.Is(keyLookupErr, sql.ErrNoRows):
			return keyLookupErr
		}
		var existingSampleID int64
		lookupErr := tx.QueryRowContext(ctx, `
SELECT sample_id FROM corpus_set_samples WHERE corpus_set_id = $1 AND sample_key = $2`,
			setID, p.input.Key).Scan(&existingSampleID)
		switch {
		case lookupErr == nil && existingSampleID != sampleID:
			return fmt.Errorf("%w: sample key already refers to different content", ErrCorpusConflict)
		case lookupErr != nil && !errors.Is(lookupErr, sql.ErrNoRows):
			return lookupErr
		}
		_, err = tx.ExecContext(ctx, `
INSERT INTO corpus_set_samples (corpus_set_id, sample_id, sample_key, name)
VALUES ($1, $2, $3, $4)
ON CONFLICT (corpus_set_id, sample_key)
DO UPDATE SET sample_id = EXCLUDED.sample_id, name = EXCLUDED.name`,
			setID, sampleID, p.input.Key, name)
		if err != nil {
			return fmt.Errorf("insert corpus membership: %w", err)
		}
	}
	for _, key := range mutation.DeleteKeys {
		_, err := tx.ExecContext(ctx, `DELETE FROM corpus_set_samples WHERE corpus_set_id = $1 AND sample_key = $2`, setID, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *PGStore) CreateCorpusDraft(ctx context.Context, pkg, name string, version int, baseVersion *int, mutation CorpusMutation) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkgID, err := s.corpusPackageID(ctx, tx, pkg)
	if err != nil {
		return err
	}
	if version <= 0 {
		return fmt.Errorf("%w: version must be positive", ErrCorpusConflict)
	}
	var existing int
	err = tx.QueryRowContext(ctx, `
SELECT 1 FROM corpus_sets WHERE package_id = $1 AND name = $2 AND version = $3`, pkgID, name, version).Scan(&existing)
	if err == nil {
		return ErrCorpusConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
		var baseID sql.NullInt64
		var baseStatus string
		if baseVersion != nil {
			err = tx.QueryRowContext(ctx, `
SELECT id, status FROM corpus_sets WHERE package_id = $1 AND name = $2 AND version = $3`,
				pkgID, name, *baseVersion).Scan(&baseID, &baseStatus)
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			if baseStatus != CorpusStatusSealed {
				return ErrSealed
			}
		}
	var setID int64
	err = tx.QueryRowContext(ctx, `
INSERT INTO corpus_sets (package_id, name, version, status, base_id)
VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		pkgID, name, version, CorpusStatusDraft, baseID).Scan(&setID)
	if err != nil {
		return fmt.Errorf("create corpus draft: %w", err)
	}
	if baseID.Valid {
		_, err = tx.ExecContext(ctx, `
INSERT INTO corpus_set_samples (corpus_set_id, sample_id, sample_key, name, created_at)
SELECT $1, sample_id, sample_key, name, now() FROM corpus_set_samples WHERE corpus_set_id = $2`,
			setID, baseID.Int64)
		if err != nil {
			return fmt.Errorf("clone base corpus: %w", err)
		}
	}
	if err := s.applyCorpusMutationTx(ctx, tx, pkgID, setID, pkg, mutation); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *PGStore) MutateCorpusDraft(ctx context.Context, pkg, name string, version int, mutation CorpusMutation) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkgID, err := s.corpusPackageID(ctx, tx, pkg)
	if err != nil {
		return err
	}
	var setID int64
	var status string
	err = tx.QueryRowContext(ctx, `
SELECT id, status FROM corpus_sets WHERE package_id = $1 AND name = $2 AND version = $3`,
		pkgID, name, version).Scan(&setID, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status != CorpusStatusDraft {
		return ErrSealed
	}
	if err := s.applyCorpusMutationTx(ctx, tx, pkgID, setID, pkg, mutation); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *PGStore) SealCorpusDraft(ctx context.Context, pkg, name string, version int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkgID, err := s.corpusPackageID(ctx, tx, pkg)
	if err != nil {
		return err
	}
	var setID int64
	var status string
	err = tx.QueryRowContext(ctx, `
SELECT id, status FROM corpus_sets WHERE package_id = $1 AND name = $2 AND version = $3 FOR UPDATE`,
		pkgID, name, version).Scan(&setID, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status != CorpusStatusDraft {
		return ErrSealed
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM corpus_set_samples WHERE corpus_set_id = $1`, setID).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("%w: cannot seal an empty corpus", ErrCorpusConflict)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE corpus_sets SET status = 'SEALED', sealed_at = now() WHERE id = $1`, setID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *PGStore) DeleteCorpusDraft(ctx context.Context, pkg, name string, version int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkgID, err := s.corpusPackageID(ctx, tx, pkg)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
DELETE FROM corpus_sets
WHERE package_id = $1 AND name = $2 AND version = $3 AND status = 'DRAFT'`,
		pkgID, name, version)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var status string
		err = tx.QueryRowContext(ctx, `SELECT status FROM corpus_sets WHERE package_id = $1 AND name = $2 AND version = $3`, pkgID, name, version).Scan(&status)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err == nil {
			return ErrSealed
		}
	}
	return tx.Commit()
}

func (s *PGStore) GetCorpusSet(ctx context.Context, pkg, name string, version int) (*CorpusSet, error) {
	q := selectCorpusSet + ` WHERE p.name = $1 AND cs.name = $2 AND cs.version = $3`
	row := s.db.QueryRowContext(ctx, q, pkg, name, version)
	var set CorpusSet
	var gotPkg, gotName string
	if err := scanCorpusSet(row, &set, &gotPkg, &gotName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	set.Package, set.Name = gotPkg, gotName
	return &set, nil
}

func (s *PGStore) LatestCorpusSet(ctx context.Context, pkg, name string) (*CorpusSet, error) {
	row := s.db.QueryRowContext(ctx, selectCorpusSet+`
WHERE p.name = $1 AND cs.name = $2 ORDER BY cs.version DESC LIMIT 1`, pkg, name)
	var set CorpusSet
	var gotPkg, gotName string
	if err := scanCorpusSet(row, &set, &gotPkg, &gotName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	set.Package, set.Name = gotPkg, gotName
	return &set, nil
}

func (s *PGStore) ListCorpusSets(ctx context.Context, pkg, name string) ([]CorpusSet, error) {
	rows, err := s.db.QueryContext(ctx, selectCorpusSet+`
WHERE p.name = $1 AND ($2 = '' OR cs.name = $2) ORDER BY cs.name, cs.version`, pkg, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CorpusSet
	if name != "" {
		// The query above cannot return a no-row error into a nil slice; keep
		// JSON responses stable as [] rather than null when the package has no
		// corpora with this name.
		out = []CorpusSet{}
	}
	for rows.Next() {
		var set CorpusSet
		if err := scanCorpusSet(rows, &set, &set.Package, &set.Name); err != nil {
			return nil, err
		}
		out = append(out, set)
	}
	return out, rows.Err()
}

func (s *PGStore) ListCorpusSamples(ctx context.Context, setID int64) ([]CorpusSampleEnvelope, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT css.corpus_set_id, css.sample_id, css.sample_key, css.name, css.created_at,
       cs.package_id, cs2.fingerprint, cs2.name, cs2.message_name, cs2.encoding,
       cs2.data, cs2.expected_result, cs2.content_summary, cs2.created_at,
       p.name
FROM corpus_set_samples css
JOIN corpus_samples cs2 ON cs2.id = css.sample_id
JOIN corpus_sets cs ON cs.id = css.corpus_set_id
JOIN packages p ON p.id = cs2.package_id
WHERE css.corpus_set_id = $1 ORDER BY css.sample_key`, setID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CorpusSampleEnvelope
	for rows.Next() {
		var env CorpusSampleEnvelope
		var packageID int64
		var expected []byte
		if err := rows.Scan(
			&env.Membership.CorpusSetID, &env.Membership.SampleID, &env.Membership.Key, &env.Membership.Name, &env.Membership.CreatedAt,
			&packageID, &env.Sample.Fingerprint, &env.Sample.Name, &env.Sample.Message, &env.Sample.Encoding,
			&env.Sample.Data, &expected, &env.Sample.ContentSummary, &env.Sample.CreatedAt, &env.Sample.Package,
		); err != nil {
			return nil, err
		}
		env.Sample.ID = env.Membership.SampleID
		if err := json.Unmarshal(expected, &env.Sample.ExpectedResult); err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, rows.Err()
}

func (s *PGStore) GetCorpusSample(ctx context.Context, pkg string, sampleID int64) (*CorpusSample, error) {
	var s0 CorpusSample
	var expected []byte
	err := s.db.QueryRowContext(ctx, `
SELECT cs.id, p.name, cs.fingerprint, cs.name, cs.message_name, cs.encoding,
       cs.data, cs.expected_result, cs.content_summary, cs.created_at
FROM corpus_samples cs JOIN packages p ON p.id = cs.package_id
WHERE p.name = $1 AND cs.id = $2`, pkg, sampleID).
		Scan(&s0.ID, &s0.Package, &s0.Fingerprint, &s0.Name, &s0.Message, &s0.Encoding,
			&s0.Data, &expected, &s0.ContentSummary, &s0.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(expected, &s0.ExpectedResult); err != nil {
		return nil, err
	}
	return &s0, nil
}

func replayIDFromRow(row interface {
	Scan(dest ...any) error
}) (*Replay, error) {
	var r Replay
	var pkgName, corpusName, schemaVersion string
	var corpusID, packageID, schemaID int64
	var lease sql.NullTime
	var errText string
	if err := row.Scan(&r.ID, &packageID, &corpusID, &schemaID, &r.Status, &r.Total, &r.Succeeded,
		&r.Failed, &r.MissingFieldItems, &r.JSONDiffItems, &lease, &errText, &r.StartedAt, &r.UpdatedAt,
		&pkgName, &corpusName, &r.CorpusVersion, &schemaVersion); err != nil {
		return nil, err
	}
	r.Package, r.CorpusName, r.SchemaVersion = pkgName, corpusName, schemaVersion
	if lease.Valid {
		t := lease.Time
		r.LeaseExpiresAt = &t
	}
	r.LastUnclaimedError = errText
	return &r, nil
}

const replaySelect = `
SELECT r.id, r.package_id, r.corpus_set_id, r.schema_version_id, r.status, r.total, r.succeeded,
       r.failed, r.missing_field_items, r.json_diff_items, r.lease_expires_at, r.last_error,
       r.started_at, r.updated_at, p.name, cs.name, cs.version, v.version
FROM replays r
JOIN packages p ON p.id = r.package_id
JOIN corpus_sets cs ON cs.id = r.corpus_set_id
JOIN versions v ON v.id = r.schema_version_id`

func (s *PGStore) StartReplay(ctx context.Context, in Replay) (*Replay, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()
	var pkgID, corpusID, schemaID int64
	err = tx.QueryRowContext(ctx, `
SELECT p.id, cs.id, v.id
FROM packages p
JOIN corpus_sets cs ON cs.package_id = p.id
JOIN versions v ON v.package_id = p.id
WHERE p.name = $1 AND cs.name = $2 AND cs.version = $3 AND v.version = $4 AND cs.status = 'SEALED'
FOR UPDATE OF cs, v`,
		in.Package, in.CorpusName, in.CorpusVersion, in.SchemaVersion).Scan(&pkgID, &corpusID, &schemaID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, ErrNotFound
	}
	if err != nil {
		return nil, false, err
	}
	created := true
	err = tx.QueryRowContext(ctx, `
INSERT INTO replays (id, package_id, corpus_set_id, schema_version_id, status)
VALUES ($1, $2, $3, $4, 'PENDING')
ON CONFLICT (corpus_set_id, schema_version_id) DO NOTHING
RETURNING id`, in.ID, pkgID, corpusID, schemaID).Scan(&in.ID)
	if errors.Is(err, sql.ErrNoRows) {
		created = false
	} else if err != nil {
		return nil, false, err
	}
	if created {
		_, err = tx.ExecContext(ctx, `
INSERT INTO replay_results (replay_id, sample_id, sample_key, status)
SELECT $1, css.sample_id, css.sample_key, 'PENDING'
FROM corpus_set_samples css WHERE css.corpus_set_id = $2
ON CONFLICT DO NOTHING`, in.ID, corpusID)
		if err != nil {
			return nil, false, err
		}
		_, err = tx.ExecContext(ctx, `
UPDATE replays SET total = (SELECT count(*) FROM replay_results WHERE replay_id = $1) WHERE id = $1`, in.ID)
		if err != nil {
			return nil, false, err
		}
	}
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		got, err := s.GetReplay(ctx, in.ID)
		if err != nil {
			return nil, false, err
		}
		return got, created, nil
}

func (s *PGStore) GetReplay(ctx context.Context, replayID string) (*Replay, error) {
	row := s.db.QueryRowContext(ctx, replaySelect+` WHERE r.id = $1`, replayID)
	r, err := replayIDFromRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return r, err
}

func (s *PGStore) GetReplayByTarget(ctx context.Context, pkg, corpusName, schemaVersion string, corpusVersion int) (*Replay, error) {
	row := s.db.QueryRowContext(ctx, replaySelect+`
WHERE p.name = $1 AND cs.name = $2 AND cs.version = $3 AND v.version = $4`,
		pkg, corpusName, corpusVersion, schemaVersion)
	r, err := replayIDFromRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return r, err
}

func (s *PGStore) ListReplays(ctx context.Context, pkg, corpusName string, corpusVersion *int) ([]Replay, error) {
	rows, err := s.db.QueryContext(ctx, replaySelect+`
WHERE p.name = $1 AND cs.name = $2 AND ($3::bigint IS NULL OR cs.version = $3)
ORDER BY r.id`, pkg, corpusName, nullableIntPointer(corpusVersion))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Replay
	for rows.Next() {
		r, err := replayIDFromRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

func nullableIntPointer(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}

func nonNilStrings(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

func nonNilDiffs(in []replay.JSONDiff) []replay.JSONDiff {
	if in == nil {
		return []replay.JSONDiff{}
	}
	return in
}

func (s *PGStore) ListReplayResults(ctx context.Context, replayID string) ([]ReplayItemResult, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT replay_id, sample_id, sample_key, status, error, missing_fields, json_diffs, decoded_json, updated_at
FROM replay_results WHERE replay_id = $1 ORDER BY sample_key`, replayID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReplayItemResult
	for rows.Next() {
		var item ReplayItemResult
		var missing, diffs []byte
		if err := rows.Scan(&item.ReplayID, &item.SampleID, &item.Key, &item.Status, &item.Error, &missing, &diffs, &item.DecodedJSON, &item.UpdatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(missing, &item.MissingFields); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(diffs, &item.JSONDiffs); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PGStore) ClaimReplayItem(ctx context.Context, replayID string, now, leaseExpires time.Time) (*ClaimedReplay, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var status string
	var lease sql.NullTime
	err = tx.QueryRowContext(ctx, `SELECT status, lease_expires_at FROM replays WHERE id = $1 FOR UPDATE`, replayID).Scan(&status, &lease)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if status == ReplayStatusComplete || status == ReplayStatusFailed {
		return nil, ErrNotFound
	}
	var item ReplayItem
	err = tx.QueryRowContext(ctx, `
SELECT sample_id, sample_key FROM replay_results
WHERE replay_id = $1 AND (status = 'PENDING' OR status = 'RUNNING')
ORDER BY sample_key LIMIT 1 FOR UPDATE SKIP LOCKED`, replayID).Scan(&item.SampleID, &item.Key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE replay_results SET status = 'RUNNING', updated_at = $2 WHERE replay_id = $1 AND sample_id = $3`,
		replayID, now, item.SampleID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE replays SET status = 'RUNNING', lease_expires_at = $2, updated_at = $3 WHERE id = $1`, replayID, leaseExpires, now)
	if err != nil {
		return nil, err
	}
	var rep Replay
	rep.ID = replayID
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &ClaimedReplay{Replay: &rep, Item: item}, nil
}

func (s *PGStore) FinishReplayItem(ctx context.Context, replayID string, sampleID int64, result replay.Result, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM replay_results WHERE replay_id = $1 AND sample_id = $2 FOR UPDATE`, replayID, sampleID).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if status == replay.StatusSuccess || status == replay.StatusFailed {
		_ = tx.Rollback()
		return nil
	}
	missing, err := json.Marshal(nonNilStrings(result.MissingFields))
	if err != nil {
		return err
	}
	diffs, err := json.Marshal(nonNilDiffs(result.JSONDiffs))
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
UPDATE replay_results
SET status = $3, error = $4, missing_fields = $5, json_diffs = $6, decoded_json = $7, updated_at = $8
WHERE replay_id = $1 AND sample_id = $2`,
		replayID, sampleID, result.Status, result.Error, missing, diffs, result.DecodedJSON, now)
	if err != nil {
		return err
	}
	var total, succeeded, failed, missingItems, diffItems, unfinished int
	err = tx.QueryRowContext(ctx, `
SELECT count(*),
       count(*) FILTER (WHERE status = 'SUCCESS'),
       count(*) FILTER (WHERE status = 'FAILED'),
       count(*) FILTER (WHERE jsonb_array_length(missing_fields) > 0),
       count(*) FILTER (WHERE jsonb_array_length(json_diffs) > 0),
       count(*) FILTER (WHERE status IN ('PENDING', 'RUNNING'))
FROM replay_results WHERE replay_id = $1`, replayID).
		Scan(&total, &succeeded, &failed, &missingItems, &diffItems, &unfinished)
	if err != nil {
		return err
	}
	finalStatus := ReplayStatusRunning
	if unfinished == 0 {
		finalStatus = ReplayStatusComplete
	}
	_, err = tx.ExecContext(ctx, `
UPDATE replays
SET total = $2, succeeded = $3, failed = $4, missing_field_items = $5,
    json_diff_items = $6, status = $7, lease_expires_at = CASE WHEN $7 = 'COMPLETE' THEN NULL ELSE lease_expires_at END,
    updated_at = $8
WHERE id = $1`, replayID, total, succeeded, failed, missingItems, diffItems, finalStatus, now)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *PGStore) ListPendingReplays(ctx context.Context, before time.Time) ([]Replay, error) {
	rows, err := s.db.QueryContext(ctx, replaySelect+`
WHERE r.status = 'PENDING' OR (r.status = 'RUNNING' AND (r.lease_expires_at IS NULL OR r.lease_expires_at < $1))
ORDER BY r.started_at, r.id`, before)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Replay
	for rows.Next() {
		r, err := replayIDFromRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

func (s *PGStore) ReopenReplay(ctx context.Context, replayID string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE replays SET status = 'PENDING', lease_expires_at = NULL, updated_at = $2
WHERE id = $1 AND status = 'RUNNING'`, replayID, now)
	return err
}
