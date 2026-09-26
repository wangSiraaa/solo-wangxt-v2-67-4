package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCorpusHTTPEndToEnd(t *testing.T) {
	svc := NewService(NewMemStore())
	pattern, handler := svc.Handler()
	srv := httptest.NewServer(handler)
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

	if _, err := svc.RegisterVersion(context.Background(), registerReq("acme.http.corpus", "v1",
		`syntax = "proto3"; package acme.http.corpus; message M { string id = 1; }`, false)); err != nil {
		t.Fatal(err)
	}
	code, out := post(ProcedureCreateCorpusSet, map[string]any{
		"package": "acme.http.corpus", "name": "c",
		"add_or_update": []map[string]any{{
			"key": "one", "message": "acme.http.corpus.M", "encoding": "json", "data": `{"id":"x"}`,
		}},
	})
	if code != http.StatusOK {
		t.Fatalf("create status=%d body=%v", code, out)
	}
	code, out = post(ProcedureSealCorpusSet, map[string]any{"package": "acme.http.corpus", "name": "c", "version": 1})
	if code != http.StatusOK {
		t.Fatalf("seal status=%d body=%v", code, out)
	}
	code, out = post(ProcedureStartReplay, map[string]any{
		"package": "acme.http.corpus", "corpus_name": "c", "corpus_version": 1, "schema_version": "v1",
	})
	if code != http.StatusOK {
		t.Fatalf("start replay status=%d body=%v", code, out)
	}
}
