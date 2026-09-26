// Command corpusctl is the CLI front-end for the versioned sample-corpus
// and batch-replay API. It speaks ConnectRPC+JSON directly (no codegen),
// like the rest of the project.
//
//	corpusctl upload   -pkg P -corpus C [-base N] [-note T] samples.json
//	corpusctl update   -pkg P -corpus C [-note T] [-add d1,d2] [-remove d3] [samples.json]
//	corpusctl seal     -pkg P -corpus C
//	corpusctl get      -pkg P -corpus C [-version N]
//	corpusctl list     -pkg P
//	corpusctl delete-draft -pkg P -corpus C
//	corpusctl replay   -pkg P -corpus C -version N -schema V [-key K] [-wait]
//	corpusctl result   -pkg P [-id N | -key K]
//	corpusctl replays  -pkg P [-corpus C]
//	corpusctl compare  -pkg P -old ID -new ID
//
// samples.json is either a single upload object or an array of samples:
//
//	[{"name":"n","message":"acme.M","encoding":"json","data":"{...}",
//	  "expectation":{"status":"ok","json":"{...}"}}]
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const defaultAddr = "http://localhost:8080"

type client struct {
	addr string
	http *http.Client
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	c := &client{addr: envOr("REGISTRY_ADDR", defaultAddr), http: &http.Client{Timeout: 60 * time.Second}}
	var code int
	switch os.Args[1] {
	case "upload":
		code = c.upload(os.Args[2:])
	case "update":
		code = c.update(os.Args[2:])
	case "seal":
		code = c.seal(os.Args[2:])
	case "get":
		code = c.get(os.Args[2:])
	case "list":
		code = c.list(os.Args[2:])
	case "delete-draft":
		code = c.deleteDraft(os.Args[2:])
	case "replay":
		code = c.replay(os.Args[2:])
	case "result":
		code = c.result(os.Args[2:])
	case "replays":
		code = c.replays(os.Args[2:])
	case "compare":
		code = c.compare(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		code = 0
	default:
		usage()
		code = 2
	}
	os.Exit(code)
}

func usage() {
	fmt.Fprint(os.Stderr, `corpusctl — versioned sample corpora and batch replay

  upload        -pkg P -corpus C [-base N] [-note T] samples.json
  update        -pkg P -corpus C [-note T] [-add a,b] [-remove a,b] [samples.json]
  seal          -pkg P -corpus C
  get           -pkg P -corpus C [-version N]
  list          -pkg P
  delete-draft  -pkg P -corpus C
  replay        -pkg P -corpus C -version N -schema V [-key K]
  result        -pkg P (-id N | -key K)
  replays       -pkg P [-corpus C]
  compare       -pkg P -old ID -new ID

Set REGISTRY_ADDR (default `+defaultAddr+`) to point at another server.
`)
}

// loadSamples accepts either a JSON array of samples or {"samples":[...]}
// so an upload request can be captured and replayed verbatim.
func loadSamples(path string) ([]map[string]any, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var arr []map[string]any
	if err := json.Unmarshal(raw, &arr); err == nil {
		return arr, nil
	}
	var wrap struct {
		Samples []map[string]any `json:"samples"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil, fmt.Errorf("samples file must be a JSON array or {\"samples\":[...]}: %w", err)
	}
	return wrap.Samples, nil
}

func csvList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (c *client) upload(args []string) int {
	fs := flag.NewFlagSet("upload", flag.ExitOnError)
	pkg := fs.String("pkg", "", "package name")
	corpusName := fs.String("corpus", "", "corpus name")
	base := fs.Int("base", 0, "base sealed version (0 = empty)")
	note := fs.String("note", "", "corpus note")
	_ = fs.Parse(args)
	if *pkg == "" || *corpusName == "" || fs.NArg() != 1 {
		usage()
		return 2
	}
	samples, err := loadSamples(fs.Arg(0))
	if err != nil {
		return fail("load samples: %v", err)
	}
	body := map[string]any{
		"package": *pkg, "corpus": *corpusName, "base_version": *base,
		"note": *note, "samples": samples,
	}
	var out map[string]any
	if err := c.call("/registry.v1.Corpus/CreateCorpus", body, &out); err != nil {
		return fail("%v", err)
	}
	printJSON(out)
	return 0
}

func (c *client) update(args []string) int {
	fs := flag.NewFlagSet("update", flag.ExitOnError)
	pkg := fs.String("pkg", "", "package name")
	corpusName := fs.String("corpus", "", "corpus name")
	note := fs.String("note", "", "corpus note")
	add := fs.String("add", "", "comma-separated digests to add")
	remove := fs.String("remove", "", "comma-separated digests to remove")
	_ = fs.Parse(args)
	if *pkg == "" || *corpusName == "" {
		usage()
		return 2
	}
	body := map[string]any{
		"package": *pkg, "corpus": *corpusName, "note": *note,
		"add": csvList(*add), "remove": csvList(*remove),
	}
	if fs.NArg() == 1 {
		samples, err := loadSamples(fs.Arg(0))
		if err != nil {
			return fail("load samples: %v", err)
		}
		body["samples"] = samples
	}
	var out map[string]any
	if err := c.call("/registry.v1.Corpus/UpdateCorpus", body, &out); err != nil {
		return fail("%v", err)
	}
	printJSON(out)
	return 0
}

func (c *client) seal(args []string) int {
	pkg, corpusName := corpusFlags(args)
	if pkg == "" || corpusName == "" {
		usage()
		return 2
	}
	var out map[string]any
	if err := c.call("/registry.v1.Corpus/SealCorpus",
		map[string]any{"package": pkg, "corpus": corpusName}, &out); err != nil {
		return fail("%v", err)
	}
	printJSON(out)
	return 0
}

func (c *client) get(args []string) int {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	pkg := fs.String("pkg", "", "package name")
	corpusName := fs.String("corpus", "", "corpus name")
	version := fs.Int("version", 0, "version (0 = latest)")
	_ = fs.Parse(args)
	if *pkg == "" || *corpusName == "" {
		usage()
		return 2
	}
	var out map[string]any
	if err := c.call("/registry.v1.Corpus/GetCorpus",
		map[string]any{"package": *pkg, "corpus": *corpusName, "version": *version}, &out); err != nil {
		return fail("%v", err)
	}
	printJSON(out)
	return 0
}

func (c *client) list(args []string) int {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	pkg := fs.String("pkg", "", "package name")
	_ = fs.Parse(args)
	if *pkg == "" {
		usage()
		return 2
	}
	var out map[string]any
	if err := c.call("/registry.v1.Corpus/ListCorpora", map[string]any{"package": *pkg}, &out); err != nil {
		return fail("%v", err)
	}
	if corpora, ok := out["corpora"].([]any); ok {
		for _, x := range corpora {
			m := x.(map[string]any)
			fmt.Printf("%s@%v  %s  samples=%v\n", m["corpus"], m["version"], m["status"], m["sample_count"])
		}
	}
	return 0
}

func (c *client) deleteDraft(args []string) int {
	pkg, corpusName := corpusFlags(args)
	if pkg == "" || corpusName == "" {
		usage()
		return 2
	}
	var out map[string]any
	if err := c.call("/registry.v1.Corpus/DeleteCorpusDraft",
		map[string]any{"package": pkg, "corpus": corpusName}, &out); err != nil {
		return fail("%v", err)
	}
	printJSON(out)
	return 0
}

func (c *client) replay(args []string) int {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	pkg := fs.String("pkg", "", "package name")
	corpusName := fs.String("corpus", "", "corpus name")
	version := fs.Int("version", 0, "sealed corpus version")
	schemaVersion := fs.String("schema", "", "registered schema version")
	key := fs.String("key", "", "idempotency key (default: derived from inputs)")
	_ = fs.Parse(args)
	if *pkg == "" || *corpusName == "" || *version == 0 || *schemaVersion == "" {
		usage()
		return 2
	}
	body := map[string]any{
		"package": *pkg, "corpus": *corpusName, "corpus_version": *version,
		"schema_version": *schemaVersion, "replay_key": *key,
	}
	var out map[string]any
	if err := c.call("/registry.v1.Corpus/StartReplay", body, &out); err != nil {
		return fail("%v", err)
	}
	printReplay(out)
	return 0
}

func (c *client) result(args []string) int {
	fs := flag.NewFlagSet("result", flag.ExitOnError)
	pkg := fs.String("pkg", "", "package name")
	id := fs.Int64("id", 0, "replay id")
	key := fs.String("key", "", "replay key")
	_ = fs.Parse(args)
	if *pkg == "" || (*id == 0 && *key == "") {
		usage()
		return 2
	}
	var out map[string]any
	if err := c.call("/registry.v1.Corpus/GetReplay",
		map[string]any{"package": *pkg, "replay_id": *id, "replay_key": *key}, &out); err != nil {
		return fail("%v", err)
	}
	printReplay(out)
	return 0
}

func (c *client) replays(args []string) int {
	fs := flag.NewFlagSet("replays", flag.ExitOnError)
	pkg := fs.String("pkg", "", "package name")
	corpusName := fs.String("corpus", "", "filter by corpus")
	_ = fs.Parse(args)
	if *pkg == "" {
		usage()
		return 2
	}
	var out map[string]any
	if err := c.call("/registry.v1.Corpus/ListReplays",
		map[string]any{"package": *pkg, "corpus": *corpusName}, &out); err != nil {
		return fail("%v", err)
	}
	if runs, ok := out["replays"].([]any); ok {
		for _, x := range runs {
			m := x.(map[string]any)
			fmt.Printf("#%v %s@%v schema=%s %s %v/%v\n",
				m["id"], m["corpus"], m["corpus_version"], m["schema_version"],
				m["status"], m["completed"], m["total"])
		}
	}
	return 0
}

func (c *client) compare(args []string) int {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	pkg := fs.String("pkg", "", "package name")
	oldID := fs.Int64("old", 0, "old replay id")
	newID := fs.Int64("new", 0, "new replay id")
	_ = fs.Parse(args)
	if *pkg == "" || *oldID == 0 || *newID == 0 {
		usage()
		return 2
	}
	var out map[string]any
	if err := c.call("/registry.v1.Corpus/CompareReplays",
		map[string]any{"package": *pkg, "old_replay_id": *oldID, "new_replay_id": *newID}, &out); err != nil {
		return fail("%v", err)
	}
	cmp, _ := out["comparison"].(map[string]any)
	summary, _ := cmp["summary"].(map[string]any)
	fmt.Printf("schema %s -> %s\n", cmp["old_schema_version"], cmp["new_schema_version"])
	fmt.Printf("items %v: changed=%v regressed=%v fixed=%v new_decode_failures=%v\n",
		summary["total_items"], summary["changed_items"], summary["regressed_items"],
		summary["fixed_items"], summary["new_decode_failures"])
	if items, ok := cmp["items"].([]any); ok {
		for _, x := range items {
			it := x.(map[string]any)
			if it["changed"] != true {
				continue
			}
			tag := "changed"
			if it["regressed"] == true {
				tag = "REGRESSED"
			}
			fmt.Printf("  [%s] %s (%s/%s)\n", tag, it["name"], it["message"], it["encoding"])
			if added, ok := it["paths_added"].([]any); ok {
				for _, p := range added {
					fmt.Printf("      + %v\n", p)
				}
			}
			if removed, ok := it["paths_removed"].([]any); ok {
				for _, p := range removed {
					fmt.Printf("      - %v\n", p)
				}
			}
		}
	}
	return 0
}

func corpusFlags(args []string) (pkg, corpusName string) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	p := fs.String("pkg", "", "")
	cn := fs.String("corpus", "", "")
	_ = fs.Parse(args)
	return *p, *cn
}

func (c *client) call(procedure string, body any, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, c.addr+procedure, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s: HTTP %d: %s", procedure, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

func printReplay(out map[string]any) {
	r, _ := out["replay"].(map[string]any)
	fmt.Printf("replay #%v %s@%v schema=%s status=%s %v/%v\n",
		r["id"], r["corpus"], r["corpus_version"], r["schema_version"],
		r["status"], r["completed"], r["total"])
	if r["summary"] != nil {
		printJSON(r["summary"])
	}
	if items, ok := out["items"].([]any); ok {
		for _, x := range items {
			it := x.(map[string]any)
			fmt.Printf("  %s (%s/%s) decode_ok=%v", it["status"], it["message"], it["encoding"], it["decode_ok"])
			if name, ok := it["name"].(string); ok && name != "" {
				fmt.Printf(" name=%s", name)
			}
			fmt.Println()
			if lost, ok := it["lost_fields"].([]any); ok && len(lost) > 0 {
				for _, p := range lost {
					fmt.Printf("      lost:  %v\n", p)
				}
			}
			if diffs, ok := it["json_diffs"].([]any); ok {
				for _, d := range diffs {
					dd := d.(map[string]any)
					fmt.Printf("      json:  %v  %v -> %v\n", dd["path"], dd["old"], dd["new"])
				}
			}
			if e, ok := it["error"].(string); ok && e != "" {
				fmt.Printf("      error: %s\n", e)
			}
		}
	}
}

func printJSON(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

func fail(format string, args ...any) int {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	return 1
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
