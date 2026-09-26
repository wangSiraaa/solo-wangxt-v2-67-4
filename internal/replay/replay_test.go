package replay

import (
	"context"
	"encoding/base64"
	"testing"

	"protocompat/internal/schema"
)

func TestDecodeJSONAndWireOutcomes(t *testing.T) {
	compiled, err := schema.Compile(context.Background(), []schema.SourceFile{{Path: "m.proto", Content: `syntax = "proto3"; package acme.test; message M { string id = 1; int32 value = 2; }`}})
	if err != nil {
		t.Fatal(err)
	}
	files := compiled.Files

	ok := Decode(files, Sample{Message: "acme.test.M", Encoding: "json", Data: `{"id":"a","value":3}`})
	if ok.Status != StatusSuccess || len(ok.MissingFields) != 0 || len(ok.JSONDiffs) != 0 {
		t.Fatalf("json success = %+v", ok)
	}

	unknown := Decode(files, Sample{Message: "acme.test.M", Encoding: "json", Data: `{"id":"b","gone":true}`})
	if unknown.Status != StatusFailed || len(unknown.MissingFields) != 1 || unknown.MissingFields[0] != "gone" {
		t.Fatalf("unknown field result = %+v", unknown)
	}

	wire := Decode(files, Sample{Message: "acme.test.M", Encoding: "wire", Data: encodeTestBase64([]byte{0x08})})
	if wire.Status != StatusFailed || wire.Error == "" {
		t.Fatalf("malformed wire = %+v", wire)
	}

	expectedFailure := Decode(files, Sample{
		Message: "acme.test.M", Encoding: "wire", Data: encodeTestBase64([]byte{0x08}),
		Expected: ExpectedResult{Outcome: OutcomeFailure},
	})
	if expectedFailure.Status != StatusSuccess {
		t.Fatalf("expected failure = %+v", expectedFailure)
	}
}

func encodeTestBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func TestFingerprintCanonicalizesJSON(t *testing.T) {
	a := Sample{Message: "acme.test.M", Encoding: "json", Data: `{"id":"a","value":1}`}
	b := Sample{Message: "acme.test.M", Encoding: "json", Data: `{ "value": 1, "id": "a" }`}
	fpA, summaryA, err := Fingerprint(a)
	if err != nil {
		t.Fatal(err)
	}
	fpB, _, err := Fingerprint(b)
	if err != nil {
		t.Fatal(err)
	}
	if string(fpA) != string(fpB) {
		t.Fatal("semantically identical JSON must share fingerprint")
	}
	if summaryA == "" {
		t.Fatal("content summary missing")
	}
}
