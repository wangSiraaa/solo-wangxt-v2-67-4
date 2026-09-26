package registry

import (
	"context"
	"sync"
	"testing"

	"connectrpc.com/connect"

	"google.golang.org/protobuf/reflect/protoregistry"
	"protocompat/internal/schema"
)

// Two RunReplay workers racing over the same run must produce exactly one
// result per item, and the run must complete with every item done once.
func TestReplayConcurrentWorkersSingleResult(t *testing.T) {
	store := NewMemStore()
	cs := NewCorpusService(store)
	svc := NewService(store)
	ctx := context.Background()
	registerSchema(t, svc, "acme.evt", "v1", corpusV1Proto)

	const n = 12
	uploads := make([]CorpusUploadSample, 0, n)
	for i := 0; i < n; i++ {
		uploads = append(uploads, uploadJSON("",
			`{"id":"`+string(rune('a'+i))+`"}`))
	}
	if _, err := cs.CreateCorpus(ctx, connect.NewRequest(&CreateCorpusRequest{
		Package: "acme.evt", Corpus: "events", Samples: uploads,
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.SealCorpus(ctx, connect.NewRequest(&SealCorpusRequest{Package: "acme.evt", Corpus: "events"})); err != nil {
		t.Fatal(err)
	}
	set, _ := store.GetCorpusSet(ctx, "acme.evt", "events", 1)
	ver, _ := store.GetVersion(ctx, "acme.evt", "v1")
	items, err := cs.snapshotItems(ctx, "acme.evt", set)
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.CreateReplay(ctx, Replay{
		Package: "acme.evt", Key: "race", Corpus: "events",
		CorpusVersion: 1, SetID: set.ID, SchemaVersion: "v1",
	}, items)
	if err != nil {
		t.Fatal(err)
	}
	loader := func() (*protoregistry.Files, error) { return schema.Load(ver.DescriptorSet) }

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := RunReplay(ctx, store, run, loader); err != nil && err != ErrRunInterrupted {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("worker: %v", e)
	}

	finished, err := store.GetReplay(ctx, "acme.evt", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != ReplayCompleted || finished.Completed != n {
		t.Fatalf("run = %s %d/%d", finished.Status, finished.Completed, n)
	}
	listed, _ := store.ListReplayItems(ctx, run.ID)
	if len(listed) != n {
		t.Fatalf("items = %d", len(listed))
	}
	for _, it := range listed {
		if it.Status != ItemCompleted || it.Result == nil {
			t.Fatalf("item %d not completed: %s", it.Position, it.Status)
		}
		if it.Attempt < 1 {
			t.Fatalf("item %d never attempted", it.Position)
		}
	}
}
