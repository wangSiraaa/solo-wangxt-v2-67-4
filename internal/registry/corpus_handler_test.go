package registry

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestCorpusHTTPEndToEnd drives the corpus API over real HTTP including the
// sealed-immutability rejection and a replay.
func TestCorpusHTTPEndToEnd(t *testing.T) {
	store := NewMemStore()
	srv := httptest.NewServer(NewService(store).NewRootMux(NewCorpusService(store), nil))
	defer srv.Close()

	post := func(procedure string, body any) (int, map[string]any) {
		t.Helper()
		raw, _ := json.Marshal(body)
		resp, err := http.Post(srv.URL+procedure, "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	// Schema versions.
	code, _ := post(ProcedureRegisterVersion, map[string]any{
		"package": "acme.web", "version": "v1",
		"files": []map[string]string{{"path": "w.proto", "content": `syntax="proto3";package acme.web;message P{string id=1;int64 n=2;}`}},
	})
	if code != http.StatusOK {
		t.Fatalf("register v1: %d", code)
	}

	// Create corpus with one sample.
	code, out := post(ProcedureCreateCorpus, map[string]any{
		"package": "acme.web", "corpus": "pages",
		"samples": []map[string]any{
			{"message": "acme.web.P", "encoding": "json", "data": `{"id":"p","n":3}`},
		},
	})
	if code != http.StatusOK {
		t.Fatalf("create corpus: %d %v", code, out)
	}
	corpusBlock := out["corpus"].(map[string]any)
	if corpusBlock["version"].(float64) != 1 {
		t.Fatalf("version = %v", corpusBlock["version"])
	}

	// Seal.
	if code, out = post(ProcedureSealCorpus, map[string]any{"package": "acme.web", "corpus": "pages"}); code != http.StatusOK {
		t.Fatalf("seal: %d %v", code, out)
	}

	// Sealed update must be rejected. The connect unary+JSON protocol
	// carries the code in the error body (HTTP status stays 400).
	if code, out = post(ProcedureUpdateCorpus, map[string]any{
		"package": "acme.web", "corpus": "pages", "note": "nope",
	}); code != http.StatusBadRequest || out["code"] != "failed_precondition" {
		t.Fatalf("sealed update: %d %v", code, out)
	}

	// Replay.
	code, out = post(ProcedureStartReplay, map[string]any{
		"package": "acme.web", "corpus": "pages", "corpus_version": 1, "schema_version": "v1",
	})
	if code != http.StatusOK {
		t.Fatalf("replay: %d %v", code, out)
	}
	run := out["replay"].(map[string]any)
	if run["status"] != "completed" {
		t.Fatalf("status = %v", run["status"])
	}
	items := out["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("items = %d", len(items))
	}
	item := items[0].(map[string]any)
	if item["status"] != "JSON_DIFF" { // int64 3 vs "3"
		t.Fatalf("item status = %v, want JSON_DIFF", item["status"])
	}

	// Idempotent replay: same inputs, same run id.
	_, out2 := post(ProcedureStartReplay, map[string]any{
		"package": "acme.web", "corpus": "pages", "corpus_version": 1, "schema_version": "v1",
	})
	if out2["already_existed"] != true {
		t.Fatalf("replay not idempotent: %v", out2)
	}
}
