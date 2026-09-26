package corpus

import (
	"context"
	"testing"

	"protocompat/internal/schema"
)

func compileFiles(t *testing.T, src string) *schema.Compiled {
	t.Helper()
	c, err := schema.Compile(context.Background(), []schema.SourceFile{{Path: "m.proto", Content: src}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return c
}

func TestDecodeJSONFieldLossAndJSONBehavior(t *testing.T) {
	old := compileFiles(t, `syntax="proto3";package p;message M{string id=1;int64 total=2;string note=3;}`)
	new := compileFiles(t, `syntax="proto3";package p;message M{string id=1;int64 total=2;reserved 3;reserved "note";}`)

	// int64 renders as a quoted string -> JSON behavior diff on total;
	// "note" is unknown under the new schema -> field loss + strict decode error.
	r := Decode(new.Files, "d1", Sample{
		Message: "p.M", Encoding: "json", Data: `{"id":"x","total":5,"note":"n"}`,
	})
	if r.DecodeOK {
		t.Fatalf("strict decode must fail for unknown key note: %+v", r)
	}
	found := false
	for _, p := range r.LostFields {
		if p == "p.M.note" {
			found = true
		}
	}
	if !found {
		t.Fatalf("lost fields = %v, want p.M.note", r.LostFields)
	}

	// int64 quoting alone (old schema): decodes fine, JSON diff on total.
	r2 := Decode(old.Files, "d2", Sample{
		Message: "p.M", Encoding: "json", Data: `{"id":"x","total":5}`,
	})
	if !r2.DecodeOK {
		t.Fatalf("decode: %s", r2.Error)
	}
	if r2.Status != StatusJSONDiff {
		t.Fatalf("status = %s, want JSON_DIFF (diffs %+v)", r2.Status, r2.JSONDiffs)
	}
	sawTotal := false
	for _, d := range r2.JSONDiffs {
		if d.Path == "p.M.total" {
			sawTotal = true
			if d.Old != `"5"` || d.New != "5" {
				t.Fatalf("total diff = %q -> %q, want quoted vs raw", d.Old, d.New)
			}
		}
	}
	if !sawTotal {
		t.Fatalf("json diffs = %+v", r2.JSONDiffs)
	}
}

func TestDecodeWireFieldLoss(t *testing.T) {
	src := `syntax="proto3";package p;message M{string id=1;reserved 3;reserved "note";}`
	c := compileFiles(t, src)
	// field 1 = tag 0x0a "x" (0x78), field 3 = tag 0x1a "n" (0x6e)
	r := Decode(c.Files, "w1", Sample{
		Message: "p.M", Encoding: "wire", Data: "CgF4GgFu",
	})
	if !r.DecodeOK {
		t.Fatalf("wire unknown fields decode leniently, got error: %s", r.Error)
	}
	found := false
	for _, p := range r.LostFields {
		if p == "p.M.#3" {
			found = true
		}
	}
	if !found {
		t.Fatalf("lost = %v, want p.M.#3", r.LostFields)
	}
	if r.Status != StatusFieldLost {
		t.Fatalf("status = %s, want FIELD_LOST", r.Status)
	}
}

func TestDecodeMixedPartialFailure(t *testing.T) {
	c := compileFiles(t, `syntax="proto3";package p;message M{string id=1;}`)
	items := []Sample{
		{Message: "p.M", Encoding: "json", Data: `{"id":"ok"}`},
		{Message: "p.Missing", Encoding: "json", Data: `{}`}, // message absent in schema
		{Message: "p.M", Encoding: "wire", Data: "!!!!notbase64"},
	}
	results := make([]ItemResult, len(items))
	for i, s := range items {
		d, _ := DigestOf(s)
		results[i] = Decode(c.Files, d, s)
	}
	sum := Summarize(results)
	if sum.Total != 3 || sum.OK != 1 || sum.DecodeFailed != 2 {
		t.Fatalf("summary = %+v, want 1 ok / 2 failed of 3", sum)
	}
	if results[0].Status != StatusOK {
		t.Fatalf("item0 = %s, good item must be OK", results[0].Status)
	}
}

func TestDigestDedupFormattingInsensitive(t *testing.T) {
	a := Sample{Message: "p.M", Encoding: "json", Data: `{"id":"x","total":5}`}
	b := Sample{Message: "p.M", Encoding: "json", Data: `{ "total": 5, "id": "x" }`}
	da, err := DigestOf(a)
	if err != nil {
		t.Fatal(err)
	}
	db, _ := DigestOf(b)
	if da != db {
		t.Fatalf("same JSON content different formatting must share digest:\n%s\n%s", da, db)
	}
	c := Sample{Message: "p.M", Encoding: "json", Data: `{"id":"x","total":6}`}
	dc, _ := DigestOf(c)
	if da == dc {
		t.Fatal("different content must not share digest")
	}
}

func TestExpectationStatus(t *testing.T) {
	old := compileFiles(t, `syntax="proto3";package p;message M{string a=1;string b=2;}`)
	new := compileFiles(t, `syntax="proto3";package p;message M{oneof pick{string a=1;string b=2;}}`)
	r := Decode(new.Files, "e1", Sample{
		Message: "p.M", Encoding: "json", Data: `{"a":"1","b":"2"}`,
		Expectation: &Expectation{Status: "ok"},
	})
	if r.ExpectationMatched == nil || *r.ExpectationMatched != false {
		t.Fatalf("oneof both-set must fail ok-expectation: %+v", r)
	}

	r2 := Decode(old.Files, "e2", Sample{
		Message: "p.M", Encoding: "json", Data: `{"a":"1"}`,
		Expectation: &Expectation{JSON: `{"a":"1"}`},
	})
	if r2.ExpectationMatched == nil || !*r2.ExpectationMatched {
		t.Fatalf("canonical JSON should match expectation: %+v", r2)
	}
}

func TestCompareReplaysStable(t *testing.T) {
	c := compileFiles(t, `syntax="proto3";package p;message M{string id=1;int64 total=2;}`)
	s := Sample{Message: "p.M", Encoding: "json", Data: `{"id":"x","total":5}`}
	d, _ := DigestOf(s)
	r1 := Decode(c.Files, d, s)
	r2 := Decode(c.Files, d, s)
	cmp := CompareReplays("v1", "v2", "r1", "r2", []ItemResult{r1}, []ItemResult{r2})
	if cmp.Summary.ChangedItems != 0 {
		t.Fatalf("identical replays must compare equal: %+v", cmp.Summary)
	}
}
