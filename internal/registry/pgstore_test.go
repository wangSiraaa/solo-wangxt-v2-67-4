package registry

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/protobuf/reflect/protoregistry"

	"protocompat/internal/replay"
	"protocompat/internal/schema"
)

// TestPGStore exercises the PostgreSQL store end to end. It is skipped
// unless DATABASE_URL points at a database (see docker-compose.yml).
func TestPGStore(t *testing.T) {
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
	// Isolate the test in its own package rows.
	pkg := "test.pgstore." + time.Now().Format("20060102150405.000000000")
	store := NewPGStore(db)
	compiledV1, err := schema.Compile(ctx, []schema.SourceFile{{Path: "a.proto", Content: `syntax = "proto3"; package a; message M { int32 f = 1; }`}})
	if err != nil {
		t.Fatal(err)
	}
	compiledV2, err := schema.Compile(ctx, []schema.SourceFile{{Path: "a.proto", Content: `syntax = "proto3"; package a; message M { E f = 1; } enum E { E_UNSPECIFIED = 0; E_ONE = 1; }`}})
	if err != nil {
		t.Fatal(err)
	}

	v1 := Version{Package: pkg, Version: "v1", ContentHash: []byte("hash-v1"), DescriptorSet: []byte("set-v1"), OwnedPaths: []string{"a.proto"}}
	created, err := store.PutVersion(ctx, v1)
	if err != nil || !created {
		t.Fatalf("first put: created=%v err=%v", created, err)
	}
	// Identical retry is idempotent.
	created, err = store.PutVersion(ctx, v1)
	if err != nil || created {
		t.Fatalf("idempotent put: created=%v err=%v", created, err)
	}
	// Different content under the same version is rejected.
	created, err = store.PutVersion(ctx, Version{Package: pkg, Version: "v1", ContentHash: []byte("hash-other"), DescriptorSet: []byte("set-other"), OwnedPaths: []string{"a.proto"}})
	if err != ErrVersionConflict {
		t.Fatalf("conflicting put: created=%v err=%v, want ErrVersionConflict", created, err)
	}

	got, err := store.GetVersion(ctx, pkg, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if string(got.ContentHash) != "hash-v1" || string(got.DescriptorSet) != "set-v1" {
		t.Fatalf("stored content changed: %+v", got)
	}
	if got.OwnedPaths[0] != "a.proto" {
		t.Fatalf("owned paths = %v", got.OwnedPaths)
	}

	if _, err := store.PutVersion(ctx, Version{Package: pkg, Version: "v2", ContentHash: []byte("hash-v2"), DescriptorSet: []byte("set-v2"), OwnedPaths: []string{"a.proto"}}); err != nil {
		t.Fatal(err)
	}
	latest, err := store.LatestVersion(ctx, pkg)
	if err != nil || latest.Version != "v2" {
		t.Fatalf("latest = %+v err=%v", latest, err)
	}
	list, err := store.ListVersions(ctx, pkg)
	if err != nil || len(list) != 2 {
		t.Fatalf("list = %+v err=%v", list, err)
	}

	if err := store.UpsertConsumer(ctx, ConsumerDecl{Package: pkg, Consumer: "svc", Encoding: "json", Usages: []Usage{{Message: "a.M", Fields: []string{"f"}}}}); err != nil {
		t.Fatal(err)
	}
	decl, err := store.GetConsumer(ctx, pkg, "svc")
	if err != nil {
		t.Fatal(err)
	}
	if decl.Encoding != "json" || len(decl.Usages) != 1 || decl.Usages[0].Fields[0] != "f" {
		t.Fatalf("decl = %+v", decl)
	}

	if _, err := store.PutVersion(ctx, Version{Package: pkg, Version: "schema-v1",ContentHash: []byte("schema-v1"), DescriptorSet: compiledV1.DescriptorSet, OwnedPaths: []string{"a.proto"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutVersion(ctx, Version{Package: pkg, Version: "schema-v2", ContentHash: []byte("schema-v2"), DescriptorSet: compiledV2.DescriptorSet, OwnedPaths: []string{"a.proto"}}); err != nil {
		t.Fatal(err)
	}

	mutation := CorpusMutation{AddOrUpdate: []CorpusSampleInput{{
		Key: "ok", Message: "a.M", Encoding: "json", Data: `{"f":1}`,
	}, {
		Key: "bad", Message: "a.M", Encoding: "wire", Data: "CAA=",
	}}}
	if err := store.CreateCorpusDraft(ctx, pkg, "corpus", 1, nil, mutation); err != nil {
		t.Fatal(err)
	}
	// Identical re-upload is idempotent; changing a sealed version is rejected.
	if err := store.MutateCorpusDraft(ctx, pkg, "corpus", 1, mutation); err != nil {
		t.Fatal(err)
	}
	if err := store.SealCorpusDraft(ctx, pkg, "corpus", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.MutateCorpusDraft(ctx, pkg, "corpus", 1, mutation); err != ErrSealed {
		t.Fatalf("sealed mutation = %v, want ErrSealed", err)
	}
	set, err := store.GetCorpusSet(ctx, pkg, "corpus", 1)
	if err != nil {
		t.Fatal(err)
	}
	envelopes, err := store.ListCorpusSamples(ctx, set.ID)
	if err != nil || len(envelopes) != 2 {
		t.Fatalf("corpus samples = %d, err=%v", len(envelopes), err)
	}
	ids := make([]int64, 0, 2)
	for _, env := range envelopes {
		ids = append(ids, env.Sample.ID)
	}

	startReplay := func(schemaVersion string) string {
		t.Helper()
		id := "replay-" + schemaVersion
		rep, created, err := store.StartReplay(ctx, Replay{
			ID: id, Package: pkg, CorpusName: "corpus", CorpusVersion: 1, SchemaVersion: schemaVersion,
		})
		if err != nil || !created || rep.Total != 2 {
			t.Fatalf("start replay: rep=%+v created=%v err=%v", rep, created, err)
		}
		// Same input key must return the same replay and not duplicate results.
		if _, created, err := store.StartReplay(ctx, Replay{
			ID: id, Package: pkg, CorpusName: "corpus", CorpusVersion: 1, SchemaVersion: schemaVersion,
		}); err != nil || created {
			t.Fatalf("retry StartReplay created=%v err=%v", created, err)
		}
		return id
	}
	processReplay := func(id string, files *protoregistry.Files) {
		t.Helper()
		claimed, err := store.ClaimReplayItem(ctx, id, time.Now(), time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		sample, err := store.GetCorpusSample(ctx, pkg, claimed.Item.SampleID)
		if err != nil {
			t.Fatal(err)
		}
		result := replay.Decode(files, replay.Sample{
			Message: sample.Message, Encoding: sample.Encoding, Data: sample.Data, Expected: sample.ExpectedResult,
		})
		if err := store.FinishReplayItem(ctx, id, claimed.Item.SampleID, result, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	id := startReplay("schema-v1")
	// Simulate a crash after the first claim, then reopen the replay. The
	// remaining item is processed; the already terminal result is not touched.
	firstClaim, err := store.ClaimReplayItem(ctx, id, time.Now(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	firstSample, err := store.GetCorpusSample(ctx, pkg, firstClaim.Item.SampleID)
	if err != nil {
		t.Fatal(err)
	}
	firstResult := replay.Decode(compiledV1.Files, replay.Sample{
		Message: firstSample.Message, Encoding: firstSample.Encoding, Data: firstSample.Data, Expected: firstSample.ExpectedResult,
	})
	if err := store.FinishReplayItem(ctx, id, firstClaim.Item.SampleID, firstResult, time.Now()); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimReplayItem(ctx, id, time.Now(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReopenReplay(ctx, id, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimReplayItem(ctx, id, time.Now(), time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("reopen should allow reclaim of interrupted item: %v", err)
	}
	processReplay(id, compiledV1.Files)
	if _, err := store.ClaimReplayItem(ctx, id, time.Now(), time.Now().Add(time.Minute)); err != ErrNotFound {
		t.Fatalf("claim after complete = %v, want ErrNotFound", err)
	}
	results, err := store.ListReplayResults(ctx, id)
	if err != nil || len(results) != 2 {
		t.Fatalf("results = %d, err=%v", len(results), err)
	}
	if claimed.Item.SampleID == firstClaim.Item.SampleID {
		t.Fatal("resume picked the already completed item")
	}
	var firstStored *ReplayItemResult
	for i := range results {
		if results[i].SampleID == firstClaim.Item.SampleID {
			firstStored = &results[i]
		}
	}
	if firstStored == nil {
		t.Fatalf("completed item missing from results: %+v", results)
	}
	originalError := firstStored.Error
	if err := store.FinishReplayItem(ctx, id, firstStored.SampleID, replay.Result{Status: replay.StatusFailed, Error: "must not overwrite"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	results, _ = store.ListReplayResults(ctx, id)
	for _, result := range results {
		if result.SampleID == firstStored.SampleID && result.Error != originalError {
			t.Fatal("terminal replay result was overwritten by retry")
		}
	}
	rep, err := store.GetReplay(ctx, id)
	if err != nil || rep.Status != ReplayStatusComplete || rep.Total != 2 || rep.Succeeded != 1 || rep.Failed != 1 {
		t.Fatalf("replay = %+v, err=%v", rep, err)
	}

	// The same corpus against another schema gets its own stable trace/result.
	second := startReplay("schema-v2")
	if second == id {
		t.Fatal("schema versions must produce distinct replay IDs at the store API level")
	}
	for range ids {
		processReplay(second, compiledV2.Files)
	}
	secondRep, err := store.GetReplay(ctx, second)
	if err != nil || secondRep.Status != ReplayStatusComplete || secondRep.JSONDiffItems != 1 {
		t.Fatalf("second replay = %+v, err=%v", secondRep, err)
	}
}
