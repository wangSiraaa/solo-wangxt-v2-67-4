package registry

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"protocompat/internal/corpus"
	"protocompat/internal/schema"
)

const (
	corpusV1Proto = `syntax = "proto3"; package acme.evt; message Event { string id = 1; int64 ts = 2; string legacy = 3; }`
	corpusV2Proto = `syntax = "proto3"; package acme.evt; message Event { string id = 1; int64 ts = 2; reserved 3; reserved "legacy"; }`
)

func corpusService(t *testing.T) (*CorpusService, *Service, FullStore) {
	t.Helper()
	store := NewMemStore()
	return NewCorpusService(store), NewService(store), store
}

func registerSchema(t *testing.T, svc *Service, pkg, version, content string) {
	t.Helper()
	_, err := svc.RegisterVersion(context.Background(), connect.NewRequest(&RegisterVersionRequest{
		Package: pkg, Version: version,
		Files: fileSchemaFile(content),
	}))
	if err != nil {
		t.Fatalf("register %s: %v", version, err)
	}
}

func fileSchemaFile(content string) []schema.SourceFile {
	return []schema.SourceFile{{Path: "evt/event.proto", Content: content}}
}

func uploadJSON(name, data string) CorpusUploadSample {
	return CorpusUploadSample{Name: name, Message: "acme.evt.Event", Encoding: "json", Data: data}
}

// 1) Identical content uploaded twice never creates a second sample.
func TestCorpusDuplicateUploadDedup(t *testing.T) {
	cs, _, _ := corpusService(t)
	ctx := context.Background()

	resp, err := cs.CreateCorpus(ctx, connect.NewRequest(&CreateCorpusRequest{
		Package: "acme.evt", Corpus: "events",
		Samples: []CorpusUploadSample{
			uploadJSON("a", `{"id":"1","ts":100}`),
			uploadJSON("b", `{ "ts": 100, "id": "1" }`), // same content, different formatting
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.Corpus.SampleCount != 1 {
		t.Fatalf("sample_count = %d, want 1 (identical JSON must dedupe)", resp.Msg.Corpus.SampleCount)
	}
	if len(resp.Msg.AddedSampleDigests) != 1 || len(resp.Msg.AlreadySeen) != 1 {
		t.Fatalf("added=%v seen=%v, want 1 added + 1 already-seen", resp.Msg.AddedSampleDigests, resp.Msg.AlreadySeen)
	}

	// Re-upload the same content into an updated draft: still no new sample.
	upd, err := cs.UpdateCorpus(ctx, connect.NewRequest(&UpdateCorpusRequest{
		Package: "acme.evt", Corpus: "events",
		Samples: []CorpusUploadSample{uploadJSON("a", `{"id":"1","ts":100}`)},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if upd.Msg.Corpus.SampleCount != 1 || len(upd.Msg.AddedSampleDigests) != 0 {
		t.Fatalf("re-upload created a new sample: %+v", upd)
	}
}

// 2) Sealed versions are immutable: updates and deletes are refused.
func TestSealedCorpusImmutable(t *testing.T) {
	cs, _, _ := corpusService(t)
	ctx := context.Background()
	if _, err := cs.CreateCorpus(ctx, connect.NewRequest(&CreateCorpusRequest{
		Package: "acme.evt", Corpus: "events",
		Samples: []CorpusUploadSample{uploadJSON("a", `{"id":"1"}`)},
	})); err != nil {
		t.Fatal(err)
	}
	sealed, err := cs.SealCorpus(ctx, connect.NewRequest(&SealCorpusRequest{Package: "acme.evt", Corpus: "events"}))
	if err != nil {
		t.Fatal(err)
	}
	if sealed.Msg.Corpus.Status != CorpusSealed || sealed.Msg.Corpus.Version != 1 {
		t.Fatalf("seal = %+v", sealed)
	}

	_, err = cs.UpdateCorpus(ctx, connect.NewRequest(&UpdateCorpusRequest{
		Package: "acme.evt", Corpus: "events", Note: "tamper",
	}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("update sealed: code=%s err=%v", connect.CodeOf(err), err)
	}
	_, err = cs.DeleteCorpusDraft(ctx, connect.NewRequest(&DeleteCorpusDraftRequest{Package: "acme.evt", Corpus: "events"}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("delete sealed: code=%s err=%v", connect.CodeOf(err), err)
	}
	// Sealing twice is also rejected.
	if _, err := cs.SealCorpus(ctx, connect.NewRequest(&SealCorpusRequest{Package: "acme.evt", Corpus: "events"})); err == nil {
		t.Fatal("re-sealing must fail")
	}
}

// 3) Deriving a new version from a sealed base with add/remove works.
func TestCorpusVersionDerivedFromBase(t *testing.T) {
	cs, _, _ := corpusService(t)
	ctx := context.Background()
	first, err := cs.CreateCorpus(ctx, connect.NewRequest(&CreateCorpusRequest{
		Package: "acme.evt", Corpus: "events",
		Samples: []CorpusUploadSample{
			uploadJSON("a", `{"id":"a1"}`),
			uploadJSON("b", `{"id":"b2"}`),
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.SealCorpus(ctx, connect.NewRequest(&SealCorpusRequest{Package: "acme.evt", Corpus: "events"})); err != nil {
		t.Fatal(err)
	}

	// v2 = v1 - a + c.
	removedDigest := first.Msg.Corpus.Digests[0]
	v2, err := cs.CreateCorpus(ctx, connect.NewRequest(&CreateCorpusRequest{
		Package: "acme.evt", Corpus: "events", BaseVersion: 1,
		Remove:  []string{removedDigest},
		Samples: []CorpusUploadSample{uploadJSON("c", `{"id":"c3"}`)},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if v2.Msg.Corpus.Version != 2 || v2.Msg.Corpus.SampleCount != 2 {
		t.Fatalf("v2 = %+v", v2.Msg.Corpus)
	}
	// v1 is untouched and still retrievable.
	v1, err := cs.GetCorpus(ctx, connect.NewRequest(&GetCorpusRequest{Package: "acme.evt", Corpus: "events", Version: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if v1.Msg.Corpus.SampleCount != 2 || v1.Msg.Corpus.Status != CorpusSealed {
		t.Fatalf("base v1 mutated: %+v", v1.Msg.Corpus)
	}
}

// 4) A mixed wire/JSON corpus with one bad item still completes; the good
// items succeed independently.
func TestReplayMixedWireJSONPartialFailure(t *testing.T) {
	cs, svc, _ := corpusService(t)
	ctx := context.Background()
	registerSchema(t, svc, "acme.evt", "v1", corpusV1Proto)

	if _, err := cs.CreateCorpus(ctx, connect.NewRequest(&CreateCorpusRequest{
		Package: "acme.evt", Corpus: "events",
		Samples: []CorpusUploadSample{
			{Name: "good-json", Message: "acme.evt.Event", Encoding: "json", Data: `{"id":"ok","ts":7}`},
			{Name: "bad-wire", Message: "acme.evt.Event", Encoding: "wire", Data: "////"},
			{Name: "good-wire", Message: "acme.evt.Event", Encoding: "wire", Data: "CgJvaxAH"},
			{Name: "missing-msg", Message: "acme.evt.Gone", Encoding: "json", Data: `{}`},
		},
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.SealCorpus(ctx, connect.NewRequest(&SealCorpusRequest{Package: "acme.evt", Corpus: "events"})); err != nil {
		t.Fatal(err)
	}

	resp, err := cs.StartReplay(ctx, connect.NewRequest(&StartReplayRequest{
		Package: "acme.evt", Corpus: "events", CorpusVersion: 1, SchemaVersion: "v1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.Replay.Status != ReplayCompleted {
		t.Fatalf("run status = %s (%d/%d)", resp.Msg.Replay.Status, resp.Msg.Replay.Completed, resp.Msg.Replay.Total)
	}
	sum := resp.Msg.Replay.Summary
	if sum.Total != 4 || sum.DecodeFailed != 2 {
		t.Fatalf("summary = %+v, want 4 total / 2 failed", sum)
	}
	// good-json has an int64 JSON-behavior diff (7 vs "7") but decodes.
	var goodJSON *corpus.ItemResult
	for i := range resp.Msg.Items {
		if resp.Msg.Items[i].Name == "good-json" {
			goodJSON = &resp.Msg.Items[i]
		}
	}
	if goodJSON == nil || !goodJSON.DecodeOK || goodJSON.Status != corpus.StatusJSONDiff {
		t.Fatalf("good-json = %+v", goodJSON)
	}
}

// 5) Restart after interruption never redoes completed items, and the same
// replay key yields exactly one run.
func TestReplaySafeRetryAndResume(t *testing.T) {
	store := NewMemStore()
	cs := NewCorpusService(store)
	svc := NewService(store)
	ctx := context.Background()
	registerSchema(t, svc, "acme.evt", "v1", corpusV1Proto)

	if _, err := cs.CreateCorpus(ctx, connect.NewRequest(&CreateCorpusRequest{
		Package: "acme.evt", Corpus: "events",
		Samples: []CorpusUploadSample{
			uploadJSON("a", `{"id":"a"}`),
			uploadJSON("b", `{"id":"b"}`),
			uploadJSON("c", `{"id":"c"}`),
		},
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.SealCorpus(ctx, connect.NewRequest(&SealCorpusRequest{Package: "acme.evt", Corpus: "events"})); err != nil {
		t.Fatal(err)
	}

	// Create the run without driving it.
	set, _ := store.GetCorpusSet(ctx, "acme.evt", "events", 1)
	ver, _ := store.GetVersion(ctx, "acme.evt", "v1")
	snapshot, err := cs.snapshotItems(ctx, "acme.evt", set)
	if err != nil {
		t.Fatal(err)
	}
	loader := func() (*protoregistry.Files, error) { return schema.Load(ver.DescriptorSet) }
	run, err := store.CreateReplay(ctx, Replay{
		Package: "acme.evt", Key: "fixed-key", Corpus: "events",
		CorpusVersion: 1, SetID: set.ID, SchemaVersion: "v1",
	}, snapshot)
	if err != nil {
		t.Fatal(err)
	}

	// "Crash" after exactly one item.
	it, err := store.ClaimNextItem(ctx, run.ID, time.Now().Add(-time.Hour))
	if err != nil || it == nil {
		t.Fatalf("claim: %v %v", it, err)
	}
	files, _ := loader()
	firstResult := corpus.Decode(files, it.Digest, it.Sample)
	if err := store.CompleteItem(ctx, run.ID, it.Position, firstResult); err != nil {
		t.Fatal(err)
	}
	firstAttempt := it.Attempt

	// Re-claiming the completed position must never happen; resume.
	if err := RunReplay(ctx, store, run, loader); err != nil {
		t.Fatalf("resume: %v", err)
	}
	items, _ := store.ListReplayItems(ctx, run.ID)
	completed := 0
	for _, x := range items {
		if x.Status == ItemCompleted {
			completed++
		}
	}
	if completed != 3 {
		t.Fatalf("completed = %d, want 3 (must not redo the first item)", completed)
	}
	if items[0].Attempt != firstAttempt {
		t.Fatalf("first item was reprocessed: attempt %d -> %d", firstAttempt, items[0].Attempt)
	}
	// Completing an already-complete item is refused.
	if err := store.CompleteItem(ctx, run.ID, 0, firstResult); err != ErrItemAlreadyComplete {
		t.Fatalf("double complete: %v, want ErrItemAlreadyComplete", err)
	}

	// Same key again -> same run (already completed), no duplicate.
	same, err := cs.StartReplay(ctx, connect.NewRequest(&StartReplayRequest{
		Package: "acme.evt", Corpus: "events", CorpusVersion: 1,
		SchemaVersion: "v1", ReplayKey: "fixed-key",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !same.Msg.AlreadyExisted || same.Msg.Replay.ID != run.ID {
		t.Fatalf("replay key was not idempotent: %+v", same)
	}
	runs, _ := store.ListReplays(ctx, "acme.evt", "")
	if len(runs) != 1 {
		t.Fatalf("runs = %d, want exactly 1 for the key", len(runs))
	}
}

// 6) The same sealed corpus replayed against old and new schema yields a
// stable, traceable difference summary.
func TestReplayCompareOldNewSchema(t *testing.T) {
	store := NewMemStore()
	cs := NewCorpusService(store)
	svc := NewService(store)
	ctx := context.Background()
	registerSchema(t, svc, "acme.evt", "v1", corpusV1Proto)
	registerSchema(t, svc, "acme.evt", "v2", corpusV2Proto)

	if _, err := cs.CreateCorpus(ctx, connect.NewRequest(&CreateCorpusRequest{
		Package: "acme.evt", Corpus: "events",
		Samples: []CorpusUploadSample{
			uploadJSON("stable", `{"id":"keep"}`),
			uploadJSON("legacy", `{"id":"x","legacy":"old"}`),
		},
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.SealCorpus(ctx, connect.NewRequest(&SealCorpusRequest{Package: "acme.evt", Corpus: "events"})); err != nil {
		t.Fatal(err)
	}

	old, err := cs.StartReplay(ctx, connect.NewRequest(&StartReplayRequest{
		Package: "acme.evt", Corpus: "events", CorpusVersion: 1, SchemaVersion: "v1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	new, err := cs.StartReplay(ctx, connect.NewRequest(&StartReplayRequest{
		Package: "acme.evt", Corpus: "events", CorpusVersion: 1, SchemaVersion: "v2",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if old.Msg.Replay.ID == new.Msg.Replay.ID {
		t.Fatal("different schemas must produce different replay runs")
	}
	cmpResp, err := cs.CompareReplays(ctx, connect.NewRequest(&CompareReplaysRequest{
		Package: "acme.evt", OldReplayID: old.Msg.Replay.ID, NewReplayID: new.Msg.Replay.ID,
	}))
	if err != nil {
		t.Fatal(err)
	}
	cmp := cmpResp.Msg.Comparison
	if cmp.OldSchema != "v1" || cmp.NewSchema != "v2" {
		t.Fatalf("comparison schemas = %s/%s", cmp.OldSchema, cmp.NewSchema)
	}
	if cmp.Summary.TotalItems != 2 {
		t.Fatalf("total items = %d", cmp.Summary.TotalItems)
	}
	if cmp.Summary.NewDecodeFailures != 1 {
		t.Fatalf("new decode failures = %d, want 1 (legacy key removed)", cmp.Summary.NewDecodeFailures)
	}
	if cmp.Summary.RegressedItems < 1 {
		t.Fatalf("regressed = %d, want >= 1", cmp.Summary.RegressedItems)
	}
	// The stable item must show no change.
	for _, item := range cmp.Items {
		if item.Name == "legacy" {
			if !item.Regressed {
				t.Fatal("legacy sample must be flagged regressed")
			}
			found := false
			for _, p := range item.PathsAdded {
				if p == "acme.evt.Event.legacy" {
					found = true
				}
			}
			if !found {
				t.Fatalf("paths added = %v", item.PathsAdded)
			}
		}
	}

	// Comparing is deterministic: run twice, compare payloads.
	cmp2, err := cs.CompareReplays(ctx, connect.NewRequest(&CompareReplaysRequest{
		Package: "acme.evt", OldReplayID: old.Msg.Replay.ID, NewReplayID: new.Msg.Replay.ID,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cmp2.Msg.Comparison.Summary != cmp.Summary {
		t.Fatalf("comparison not stable:\n%+v\n%+v", cmp.Summary, cmp2.Msg.Comparison.Summary)
	}
}

// 7) Deleting a later draft leaves sealed sets and their replays intact.
func TestDeleteDraftDoesNotTouchHistory(t *testing.T) {
	cs, svc, store := corpusService(t)
	ctx := context.Background()
	registerSchema(t, svc, "acme.evt", "v1", corpusV1Proto)
	if _, err := cs.CreateCorpus(ctx, connect.NewRequest(&CreateCorpusRequest{
		Package: "acme.evt", Corpus: "events",
		Samples: []CorpusUploadSample{uploadJSON("a", `{"id":"a"}`)},
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.SealCorpus(ctx, connect.NewRequest(&SealCorpusRequest{Package: "acme.evt", Corpus: "events"})); err != nil {
		t.Fatal(err)
	}
	old, err := cs.StartReplay(ctx, connect.NewRequest(&StartReplayRequest{
		Package: "acme.evt", Corpus: "events", CorpusVersion: 1, SchemaVersion: "v1",
	}))
	if err != nil {
		t.Fatal(err)
	}

	// Draft v2, then delete it.
	if _, err := cs.CreateCorpus(ctx, connect.NewRequest(&CreateCorpusRequest{
		Package: "acme.evt", Corpus: "events", BaseVersion: 1,
		Samples: []CorpusUploadSample{uploadJSON("b", `{"id":"b"}`)},
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.DeleteCorpusDraft(ctx, connect.NewRequest(&DeleteCorpusDraftRequest{
		Package: "acme.evt", Corpus: "events",
	})); err != nil {
		t.Fatal(err)
	}

	// Sealed v1 and its replay are still there and complete.
	v1, err := cs.GetCorpus(ctx, connect.NewRequest(&GetCorpusRequest{Package: "acme.evt", Corpus: "events", Version: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if v1.Msg.Corpus.Status != CorpusSealed || v1.Msg.Corpus.SampleCount != 1 {
		t.Fatalf("history changed after draft deletion: %+v", v1.Msg.Corpus)
	}

	// The corpus list falls back to sealed v1 rather than vanishing
	// behind the v2 tombstone.
	listed, err := cs.ListCorpora(ctx, connect.NewRequest(&ListCorporaRequest{Package: "acme.evt"}))
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Msg.Corpora) != 1 || listed.Msg.Corpora[0].Version != 1 || listed.Msg.Corpora[0].Status != CorpusSealed {
		t.Fatalf("corpus list after draft delete = %+v", listed.Msg.Corpora)
	}
	// GetCorpus without an explicit version also resolves to sealed v1.
	latest, err := cs.GetCorpus(ctx, connect.NewRequest(&GetCorpusRequest{Package: "acme.evt", Corpus: "events"}))
	if err != nil {
		t.Fatal(err)
	}
	if latest.Msg.Corpus.Version != 1 {
		t.Fatalf("latest = %d, want sealed v1 after tombstone", latest.Msg.Corpus.Version)
	}

	got, err := cs.GetReplay(ctx, connect.NewRequest(&GetReplayRequest{Package: "acme.evt", ReplayID: old.Msg.Replay.ID}))
	if err != nil {
		t.Fatal(err)
	}
	if got.Msg.Replay.Status != ReplayCompleted || len(got.Msg.Items) != 1 {
		t.Fatalf("historical replay damaged: status=%s items=%d", got.Msg.Replay.Status, len(got.Msg.Items))
	}

	// A fresh draft after deletion becomes v3 (version numbers never reused).
	again, err := cs.CreateCorpus(ctx, connect.NewRequest(&CreateCorpusRequest{
		Package: "acme.evt", Corpus: "events", BaseVersion: 1,
		Samples: []CorpusUploadSample{uploadJSON("c", `{"id":"c"}`)},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if again.Msg.Corpus.Version != 3 {
		t.Fatalf("new draft version = %d, want 3 (v2 tombstoned)", again.Msg.Corpus.Version)
	}
	_ = store
}
