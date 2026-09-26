package registry

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"protocompat/internal/corpus"
)

// TestPGCorpusReplay exercises the corpus lifecycle and the resumable
// replay state machine against real PostgreSQL. Skipped without
// DATABASE_URL.
func TestPGCorpusReplay(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping PostgreSQL integration test")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, SchemaSQL); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	store := NewPGStore(db)
	pkg := "test.pgcorpus." + time.Now().Format("20060102150405.000000000")

	mkSample := func(data string) CorpusSampleRecord {
		return CorpusSampleRecord{
			Message: "p.M", Encoding: corpus.EncodingJSON, Data: data,
		}
	}

	// Two uploads, second identical: exactly one sample row.
	s1 := mkSample(`{"a":1}`)
	d1, err := corpus.DigestOf(corpus.Sample{Message: "p.M", Encoding: "json", Data: s1.Data})
	if err != nil {
		t.Fatal(err)
	}
	s1.Digest = d1
	created, err := store.PutSample(ctx, pkg, s1)
	if err != nil || !created {
		t.Fatalf("put sample: created=%v err=%v", created, err)
	}
	created, err = store.PutSample(ctx, pkg, s1)
	if err != nil || created {
		t.Fatalf("identical re-upload: created=%v err=%v", created, err)
	}

	s2 := mkSample(`{"a":2}`)
	d2, _ := corpus.DigestOf(corpus.Sample{Message: "p.M", Encoding: "json", Data: s2.Data})
	s2.Digest = d2
	if _, err := store.PutSample(ctx, pkg, s2); err != nil {
		t.Fatal(err)
	}

	// v1 draft = {d1, d2}, seal it.
	v1, created, err := store.CreateDraft(ctx, pkg, "events", 0, "", []string{d1, d2}, nil)
	if err != nil || !created || v1.Version != 1 {
		t.Fatalf("create v1: %+v created=%v err=%v", v1, created, err)
	}
	if _, err := store.SealCorpus(ctx, pkg, "events"); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Sealed mutation is refused at the store level.
	if _, err := store.UpdateDraft(ctx, pkg, "events", "x", nil, nil); err != ErrSealed {
		t.Fatalf("update sealed: %v, want ErrSealed", err)
	}

	// v2 draft derived from v1 minus d1.
	v2, _, err := store.CreateDraft(ctx, pkg, "events", 1, "", nil, []string{d1})
	if err != nil {
		t.Fatalf("create v2: %v", err)
	}
	if v2.Version != 2 || v2.SampleCount != 1 || v2.Digests[0] != d2 {
		t.Fatalf("v2 = %+v", v2)
	}

	// Create a replay against the sealed v1 with two pending items.
	items := []ReplayItem{
		{Digest: d1, Sample: corpus.Sample{Message: "p.M", Encoding: "json", Data: s1.Data}},
		{Digest: d2, Sample: corpus.Sample{Message: "p.M", Encoding: "json", Data: s2.Data}},
	}
	run, err := store.CreateReplay(ctx, Replay{
		Package: pkg, Key: "k1", Corpus: "events", CorpusVersion: 1,
		SetID: v1.ID, SchemaVersion: "vX",
	}, items)
	if err != nil {
		t.Fatalf("create replay: %v", err)
	}

	// Same key -> ReplayExistenceError carrying the same id.
	dup, err := store.CreateReplay(ctx, Replay{
		Package: pkg, Key: "k1", Corpus: "events", CorpusVersion: 1,
		SetID: v1.ID, SchemaVersion: "vX",
	}, items)
	if err == nil || dup.ID != run.ID {
		t.Fatalf("duplicate replay key: run=%+v err=%v", dup, err)
	}

	// Claim + complete item 0; a concurrent second claim must get item 1
	// (FOR UPDATE SKIP LOCKED semantics; items are claimed in order).
	it0, err := store.ClaimNextItem(ctx, run.ID, time.Now().Add(-time.Minute))
	if err != nil || it0 == nil || it0.Position != 0 {
		t.Fatalf("claim 0: %+v %v", it0, err)
	}
	res0 := corpus.ItemResult{Digest: d1, Message: "p.M", Encoding: "json", DecodeOK: true, Status: corpus.StatusOK}
	if err := store.CompleteItem(ctx, run.ID, 0, res0); err != nil {
		t.Fatalf("complete 0: %v", err)
	}
	// Double completion refused.
	if err := store.CompleteItem(ctx, run.ID, 0, res0); err != ErrItemAlreadyComplete {
		t.Fatalf("double complete: %v", err)
	}

	// Simulate a crash on item 1: claim it, then "die". A restart with a
	// fresh floor older than the lease must reclaim it, not skip it.
	it1, err := store.ClaimNextItem(ctx, run.ID, time.Now().Add(-time.Minute))
	if err != nil || it1 == nil || it1.Position != 1 {
		t.Fatalf("claim 1: %+v %v", it1, err)
	}
	// Immediately after claim, with a normal lease floor (30s in the
	// past), the fresh claim is still valid: nothing reclaimable.
	none, err := store.ClaimNextItem(ctx, run.ID, time.Now().Add(-30*time.Second))
	if err != nil || none != nil {
		t.Fatalf("active lease must not be reclaimable: %+v %v", none, err)
	}
	// A stale-lease floor (in the future, simulating a worker that died
	// long ago) reclaims item 1 once and bumps its attempt.
	reclaimed, err := store.ClaimNextItem(ctx, run.ID, time.Now().Add(time.Hour))
	if err != nil || reclaimed == nil || reclaimed.Position != 1 {
		t.Fatalf("reclaim stale item 1: %+v %v", reclaimed, err)
	}
	if reclaimed.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2 after reclaim", reclaimed.Attempt)
	}

	res1 := corpus.ItemResult{Digest: d2, Message: "p.M", Encoding: "json", DecodeOK: false, Status: corpus.StatusDecodeFailed, Error: "boom"}
	if err := store.CompleteItem(ctx, run.ID, 1, res1); err != nil {
		t.Fatalf("complete 1: %v", err)
	}
	// No work left.
	done, err := store.ClaimNextItem(ctx, run.ID, time.Now().Add(time.Hour))
	if err != nil || done != nil {
		t.Fatalf("queue should be empty: %+v %v", done, err)
	}

	listed, err := store.ListReplayItems(ctx, run.ID)
	if err != nil || len(listed) != 2 {
		t.Fatalf("list items: %d %v", len(listed), err)
	}
	if listed[0].Result == nil || listed[0].Result.Status != corpus.StatusOK {
		t.Fatalf("item 0 result lost: %+v", listed[0].Result)
	}
	if listed[1].Attempt != 2 {
		t.Fatalf("item 1 attempt persisted wrong: %d", listed[1].Attempt)
	}

	summary := corpus.Summarize([]corpus.ItemResult{
		*listed[0].Result, *listed[1].Result,
	})
	if err := store.CompleteReplay(ctx, run.ID, ReplayCompleted, summary); err != nil {
		t.Fatalf("complete replay: %v", err)
	}
	finished, err := store.GetReplay(ctx, pkg, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != ReplayCompleted || finished.Completed != 2 || finished.Summary == nil || finished.Summary.DecodeFailed != 1 {
		t.Fatalf("finished run = %+v", finished)
	}

	// Deleting the later draft leaves the sealed v1 and the replay.
	if err := store.DeleteDraft(ctx, pkg, "events"); err != nil {
		t.Fatalf("delete draft v2: %v", err)
	}
	if _, err := store.GetCorpusSet(ctx, pkg, "events", 1); err != nil {
		t.Fatalf("sealed v1 vanished: %v", err)
	}
	if _, err := store.GetReplay(ctx, pkg, run.ID); err != nil {
		t.Fatalf("replay vanished after draft delete: %v", err)
	}
}
