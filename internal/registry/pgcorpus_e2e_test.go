package registry

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"

	"protocompat/internal/schema"
)

// TestPGCorpusServiceEndToEnd drives the full CorpusService against real
// PostgreSQL: schema registration, corpus upload dedup, sealing, replay on
// two schema versions, and the cross-replay comparison.
func TestPGCorpusServiceEndToEnd(t *testing.T) {
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
	pkg := "test.pge2e." + time.Now().Format("20060102150405.000000000")
	svc := NewService(store)
	cs := NewCorpusService(store)

	for _, v := range []struct{ name, content string }{
		{"v1", corpusV1Proto}, {"v2", corpusV2Proto},
	} {
		if _, err := svc.RegisterVersion(ctx, connect.NewRequest(&RegisterVersionRequest{
			Package: pkg, Version: v.name,
			Files: []schema.SourceFile{{Path: "e.proto", Content: v.content}},
		})); err != nil {
			t.Fatalf("register %s: %v", v.name, err)
		}
	}

	// Duplicate-content upload (different formatting) yields one sample.
	created, err := cs.CreateCorpus(ctx, connect.NewRequest(&CreateCorpusRequest{
		Package: pkg, Corpus: "events",
		Samples: []CorpusUploadSample{
			{Name: "a", Message: "acme.evt.Event", Encoding: "json", Data: `{"id":"keep"}`},
			{Name: "dup", Message: "acme.evt.Event", Encoding: "json", Data: `{ "id": "keep" }`},
			{Name: "legacy", Message: "acme.evt.Event", Encoding: "json", Data: `{"id":"x","legacy":"o"}`},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if created.Msg.Corpus.SampleCount != 2 {
		t.Fatalf("sample_count = %d, want 2 after dedup", created.Msg.Corpus.SampleCount)
	}
	if _, err := cs.SealCorpus(ctx, connect.NewRequest(&SealCorpusRequest{Package: pkg, Corpus: "events"})); err != nil {
		t.Fatal(err)
	}

	old, err := cs.StartReplay(ctx, connect.NewRequest(&StartReplayRequest{
		Package: pkg, Corpus: "events", CorpusVersion: 1, SchemaVersion: "v1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	newRun, err := cs.StartReplay(ctx, connect.NewRequest(&StartReplayRequest{
		Package: pkg, Corpus: "events", CorpusVersion: 1, SchemaVersion: "v2",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if old.Msg.Replay.Status != "completed" || newRun.Msg.Replay.Status != "completed" {
		t.Fatalf("statuses %s %s", old.Msg.Replay.Status, newRun.Msg.Replay.Status)
	}
	if newRun.Msg.Replay.Summary.DecodeFailed != 1 {
		t.Fatalf("v2 decode failures = %d, want 1", newRun.Msg.Replay.Summary.DecodeFailed)
	}

	// Idempotent replay over PG returns the same run id.
	again, err := cs.StartReplay(ctx, connect.NewRequest(&StartReplayRequest{
		Package: pkg, Corpus: "events", CorpusVersion: 1, SchemaVersion: "v1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !again.Msg.AlreadyExisted || again.Msg.Replay.ID != old.Msg.Replay.ID {
		t.Fatalf("replay not idempotent over PG: %+v", again.Msg)
	}

	cmp, err := cs.CompareReplays(ctx, connect.NewRequest(&CompareReplaysRequest{
		Package: pkg, OldReplayID: old.Msg.Replay.ID, NewReplayID: newRun.Msg.Replay.ID,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cmp.Msg.Comparison.Summary.NewDecodeFailures != 1 {
		t.Fatalf("comparison = %+v", cmp.Msg.Comparison.Summary)
	}
}
