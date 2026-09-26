package registry

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"protocompat/internal/replay"
	"protocompat/internal/schema"
)

func TestCorpusReplayAcceptance(t *testing.T) {
	ctx := context.Background()
	svc := newService()
	if _, err := svc.RegisterVersion(ctx, registerReq("acme.corpus", "v1", v1Proto, false)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RegisterVersion(ctx, registerReq("acme.corpus", "v2", v2Proto, false)); err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Compile(ctx, []schema.SourceFile{{Path: "pay/invoice.proto", Content: v1Proto}})
	if err != nil {
		t.Fatal(err)
	}
	desc, err := compiled.Files.FindDescriptorByName("acme.pay.Invoice")
	if err != nil {
		t.Fatal(err)
	}
	md := desc.(protoreflect.MessageDescriptor)
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("id"), protoreflect.ValueOfString("wire-1"))
	msg.Set(md.Fields().ByName("total"), protoreflect.ValueOfInt32(42))
	wireBytes, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	validWire := encodeBase64(wireBytes)
	malformedWire := encodeBase64([]byte{0x08, 0x00, 0x00})

	samples := []CorpusSampleInput{
		{Key: "good-json", Message: "acme.pay.Invoice", Encoding: "json", Data: `{"id":"i-1","total":5}`},
		{Key: "missing-json", Message: "acme.pay.Invoice", Encoding: "json", Data: `{"id":"i-2","total":7,"note":"removed"}`},
		{Key: "good-wire", Message: "acme.pay.Invoice", Encoding: "wire", Data: validWire},
		{Key: "bad-wire", Message: "acme.pay.Invoice", Encoding: "wire", Data: malformedWire},
	}
	created, err := svc.CreateCorpusSet(ctx, connect.NewRequest(&CreateCorpusSetRequest{
		Package: "acme.corpus", Name: "invoices", Version: 1, AddOrUpdate: samples,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if created.Msg.Meta.Total != 4 || created.Msg.Meta.Status != CorpusStatusDraft {
		t.Fatalf("created corpus = %+v", created.Msg.Meta)
	}
	firstFingerprint := fingerprintByKey(created.Msg.Samples, "good-json")

	// Re-upload identical content under the same key must not add a sample or
	// allocate a second content-addressed sample row.
	updated, err := svc.UpdateCorpusSet(ctx, connect.NewRequest(&UpdateCorpusSetRequest{
		Package: "acme.corpus", Name: "invoices", Version: 1, AddOrUpdate: samples[:1],
	}))
	if err != nil {
		t.Fatal(err)
	}
	if updated.Msg.Meta.Total != 4 {
		t.Fatalf("identical upload changed total to %d", updated.Msg.Meta.Total)
	}
	if got := fingerprintByKey(updated.Msg.Samples, "good-json"); got != firstFingerprint {
		t.Fatalf("fingerprint changed: %s -> %s", firstFingerprint, got)
	}
	if len(svc.store.(*MemStore).corpusSamples["acme.corpus"]) != 4 {
		t.Fatalf("global content-addressed samples = %d, want 4", len(svc.store.(*MemStore).corpusSamples["acme.corpus"]))
	}

	if _, err := svc.SealCorpusSet(ctx, connect.NewRequest(&SealCorpusSetRequest{
		Package: "acme.corpus", Name: "invoices", Version: 1,
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateCorpusSet(ctx, connect.NewRequest(&UpdateCorpusSetRequest{
		Package: "acme.corpus", Name: "invoices", Version: 1,
		AddOrUpdate: []CorpusSampleInput{{Key: "new", Message: "acme.pay.Invoice", Encoding: "json", Data: `{}`}},
	})); err == nil || connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("sealed mutation error = %v, want FailedPrecondition", err)
	}

	// Start against v1, claim one item, then simulate process restart by
	// clearing the lease. Completed items must never run twice.
	v1Replay, err := svc.StartReplay(ctx, connect.NewRequest(&StartReplayRequest{
		Package: "acme.corpus", CorpusName: "invoices", CorpusVersion: 1, SchemaVersion: "v1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	v1ID := v1Replay.Msg.Summary.ReplayID
	if _, err := svc.store.ClaimReplayItem(ctx, v1ID, time.Now(), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := svc.store.ReopenReplay(ctx, v1ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if n, err := svc.ProcessPendingReplays(ctx, 0); err != nil || n != 4 {
		t.Fatalf("resume processed %d items, err=%v; want 4", n, err)
	}
	if n, err := svc.ProcessPendingReplays(ctx, 0); err != nil || n != 0 {
		t.Fatalf("second replay pass processed %d items, err=%v; completed items must not repeat", n, err)
	}
	retryStart, err := svc.StartReplay(ctx, connect.NewRequest(&StartReplayRequest{
		Package: "acme.corpus", CorpusName: "invoices", CorpusVersion: 1, SchemaVersion: "v1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if retryStart.Msg.Summary.ReplayID != v1ID || retryStart.Msg.Summary.Total != 4 || retryStart.Msg.Summary.Failed != 1 {
		t.Fatalf("idempotent v1 replay = %+v", retryStart.Msg.Summary)
	}
	v1Result, err := svc.GetReplay(ctx, connect.NewRequest(&GetReplayRequest{ReplayID: v1ID}))
	if err != nil {
		t.Fatal(err)
	}
	assertReplayItem(t, v1Result.Msg.Items, "good-json", replay.StatusSuccess, 0, 0)
	assertReplayItem(t, v1Result.Msg.Items, "missing-json", replay.StatusSuccess, 0, 0)
	assertReplayItem(t, v1Result.Msg.Items, "good-wire", replay.StatusSuccess, 0, 0)
	assertReplayItem(t, v1Result.Msg.Items, "bad-wire", replay.StatusFailed, 0, 0)

	// The same sealed corpus replayed against the newer schema gives a stable,
	// separately traceable summary: one lost field, one JSON mapping change,
	// and the malformed wire sample remains isolated as its own failure.
	v2Replay, err := svc.StartReplay(ctx, connect.NewRequest(&StartReplayRequest{
		Package: "acme.corpus", CorpusName: "invoices", CorpusVersion: 1, SchemaVersion: "v2",
	}))
	if err != nil {
		t.Fatal(err)
	}
	v2ID := v2Replay.Msg.Summary.ReplayID
	if v2ID == v1ID {
		t.Fatal("schema versions must produce distinct replay identities")
	}
	if n, err := svc.ProcessPendingReplays(ctx, 0); err != nil || n != 4 {
		t.Fatalf("v2 replay processed %d items, err=%v", n, err)
	}
	v2Result, err := svc.GetReplay(ctx, connect.NewRequest(&GetReplayRequest{ReplayID: v2ID}))
	if err != nil {
		t.Fatal(err)
	}
	summary := v2Result.Msg.Summary
	if summary.Status != ReplayStatusComplete || summary.Total != 4 || summary.Succeeded != 2 || summary.Failed != 2 ||
		summary.MissingFieldItems != 1 || summary.JSONDiffItems != 1 {
		t.Fatalf("v2 summary = %+v", summary)
	}
	assertReplayItem(t, v2Result.Msg.Items, "good-json", replay.StatusSuccess, 0, 1)
	assertReplayItem(t, v2Result.Msg.Items, "missing-json", replay.StatusFailed, 1, 0)
	assertReplayItem(t, v2Result.Msg.Items, "good-wire", replay.StatusSuccess, 0, 0)
	assertReplayItem(t, v2Result.Msg.Items, "bad-wire", replay.StatusFailed, 0, 0)
	if item := replayItem(v2Result.Msg.Items, "missing-json"); len(item.MissingFields) != 1 || item.MissingFields[0] != "note" {
		t.Fatalf("missing fields = %+v", item.MissingFields)
	}
	if item := replayItem(v2Result.Msg.Items, "good-json"); len(item.JSONDiffs) != 1 || item.JSONDiffs[0].Path != "total" {
		t.Fatalf("JSON diffs = %+v", item.JSONDiffs)
	}

	// A later draft may be deleted without changing the sealed corpus or its
	// historical replay.
	draft, err := svc.CreateCorpusSet(ctx, connect.NewRequest(&CreateCorpusSetRequest{
		Package: "acme.corpus", Name: "invoices", BaseVersion: 1,
		DeleteKeys: []string{"bad-wire"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if draft.Msg.Meta.Total != 3 {
		t.Fatalf("draft based on v1 with one delete total = %d, want 3", draft.Msg.Meta.Total)
	}
	if _, err := svc.DeleteCorpusDraft(ctx, connect.NewRequest(&DeleteCorpusDraftRequest{
		Package: "acme.corpus", Name: "invoices", Version: draft.Msg.Meta.Version,
	})); err != nil {
		t.Fatal(err)
	}
	sealed, err := svc.GetCorpusSet(ctx, connect.NewRequest(&GetCorpusSetRequest{
		Package: "acme.corpus", Name: "invoices", Version: 1,
	}))
	if err != nil || sealed.Msg.Meta.Total != 4 {
		t.Fatalf("sealed corpus after draft deletion = %+v, err=%v", sealed.Msg.Meta, err)
	}
	history, err := svc.GetReplay(ctx, connect.NewRequest(&GetReplayRequest{ReplayID: v1ID}))
	if err != nil || history.Msg.Summary.Status != ReplayStatusComplete || len(history.Msg.Items) != 4 {
		t.Fatalf("historical replay after draft deletion = %+v, err=%v", history.Msg.Summary, err)
	}
}

func encodeBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func fingerprintByKey(samples []CorpusSampleView, key string) string {
	for _, s := range samples {
		if s.Key == key {
			return s.Fingerprint
		}
	}
	return ""
}

func replayItem(items []ReplayItemView, key string) ReplayItemView {
	for _, item := range items {
		if item.Key == key {
			return item
		}
	}
	return ReplayItemView{}
}

func assertReplayItem(t *testing.T, items []ReplayItemView, key, status string, missing, diffs int) {
	t.Helper()
	item := replayItem(items, key)
	if item.Key == "" {
		t.Fatalf("item %s missing from %+v", key, items)
	}
	if item.Status != status || len(item.MissingFields) != missing || len(item.JSONDiffs) != diffs {
		t.Fatalf("item %s = %+v, want status=%s missing=%d json_diffs=%d", key, item, status, missing, diffs)
	}
}
