package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func corpusUsage() {
	fmt.Fprintf(os.Stderr, `usage:
  compatcheck corpus create -url URL -package P -name N [-file samples.json]
  compatcheck corpus update -url URL -package P -name N -version V [-file samples.json] [-delete key ...]
  compatcheck corpus seal   -url URL -package P -name N -version V
  compatcheck corpus get    -url URL -package P -name N [-version V]
  compatcheck corpus list   -url URL -package P [-name N]
  compatcheck corpus delete -url URL -package P -name N -version V
  compatcheck replay start  -url URL -package P -name N [-corpus-version V] -schema-version S [-wait]
  compatcheck replay get    -url URL -id ID | -package P -name N [-corpus-version V] -schema-version S
`)
}

func runCorpusCLI(args []string) int {
	if len(args) == 0 {
		corpusUsage()
		return 2
	}
	cmd := args[0]
	rest := args[1:]
	var err error
	switch cmd {
	case "create":
		err = corpusCreate(rest)
	case "update":
		err = corpusUpdate(rest)
	case "seal":
		err = simpleCorpusCall(rest, "seal")
	case "get":
		err = corpusGet(rest)
	case "list":
		err = corpusList(rest)
	case "delete":
		err = simpleCorpusCall(rest, "delete")
	case "start", "get":
		err = replayCLI(rest, cmd)
	default:
		corpusUsage()
		return 2
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

type commonCLIFlags struct {
	url      string
	pkg      string
	name     string
	version  int
	file     string
	schema   string
	replayID string
	wait     bool
	delete   multiFlag
}

func addCommonFlags(fs *flag.FlagSet) *commonCLIFlags {
	c := &commonCLIFlags{}
	fs.StringVar(&c.url, "url", defaultRegistryURL(), "registry base URL")
	fs.StringVar(&c.pkg, "package", "", "protobuf package")
	fs.StringVar(&c.name, "name", "", "corpus name")
	fs.IntVar(&c.version, "version", 0, "corpus version (0 means latest)")
	fs.StringVar(&c.file, "file", "", "JSON request patch / samples file")
	fs.StringVar(&c.schema, "schema-version", "", "schema version")
	fs.StringVar(&c.replayID, "id", "", "replay id")
	fs.BoolVar(&c.wait, "wait", false, "process and wait for replay completion")
	fs.Var(&c.delete, "delete", "sample key to delete (repeatable)")
	return c
}

type multiFlag []string

func (m *multiFlag) String() string { return fmt.Sprint(*m) }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func defaultRegistryURL() string {
	if u := os.Getenv("REGISTRY_URL"); u != "" {
		return u
	}
	return "http://localhost:8080"
}

func readSamplesFile(path string) ([]map[string]any, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var samples []map[string]any
	if err := json.Unmarshal(raw, &samples); err != nil {
		return nil, err
	}
	return samples, nil
}

func postRPCTo(baseURL, path string, body any) (map[string]any, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(baseURL+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if len(data) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			return nil, fmt.Errorf("decode response %q: %w", string(data), err)
		}
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(data))
	}
	return out, nil
}

func printJSON(v any) error {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

func corpusCreate(args []string) error {
	fs := flag.NewFlagSet("corpus create", flag.ContinueOnError)
	c := addCommonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	samples, err := readSamplesFile(c.file)
	if err != nil {
		return err
	}
	out, err := postRPCTo(c.url, "/registry.v1.Registry/CreateCorpusSet", map[string]any{
		"package": c.pkg, "name": c.name, "version": c.version, "add_or_update": samples,
	})
	if err != nil {
		return err
	}
	return printJSON(out)
}

func corpusUpdate(args []string) error {
	fs := flag.NewFlagSet("corpus update", flag.ContinueOnError)
	c := addCommonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	samples, err := readSamplesFile(c.file)
	if err != nil {
		return err
	}
	out, err := postRPCTo(c.url, "/registry.v1.Registry/UpdateCorpusSet", map[string]any{
		"package": c.pkg, "name": c.name, "version": c.version,
		"add_or_update": samples, "delete_keys": []string(c.delete),
	})
	if err != nil {
		return err
	}
	return printJSON(out)
}

func corpusGet(args []string) error {
	fs := flag.NewFlagSet("corpus get", flag.ContinueOnError)
	c := addCommonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	out, err := postRPCTo(c.url, "/registry.v1.Registry/GetCorpusSet", map[string]any{
		"package": c.pkg, "name": c.name, "version": c.version,
	})
	if err != nil {
		return err
	}
	return printJSON(out)
}

func corpusList(args []string) error {
	fs := flag.NewFlagSet("corpus list", flag.ContinueOnError)
	c := addCommonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	out, err := postRPCTo(c.url, "/registry.v1.Registry/ListCorpusSets", map[string]any{
		"package": c.pkg, "name": c.name,
	})
	if err != nil {
		return err
	}
	return printJSON(out)
}

func simpleCorpusCall(args []string, action string) error {
	fs := flag.NewFlagSet("corpus "+action, flag.ContinueOnError)
	c := addCommonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	procedure := "SealCorpusSet"
	if action == "delete" {
		procedure = "DeleteCorpusDraft"
	}
	out, err := postRPCTo(c.url, "/registry.v1.Registry/"+procedure, map[string]any{
		"package": c.pkg, "name": c.name, "version": c.version,
	})
	if err != nil {
		return err
	}
	return printJSON(out)
}

func replayCLI(args []string, action string) error {
	fs := flag.NewFlagSet("replay "+action, flag.ContinueOnError)
	c := addCommonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if action == "start" {
		out, err := postRPCTo(c.url, "/registry.v1.Registry/StartReplay", map[string]any{
			"package": c.pkg, "corpus_name": c.name, "corpus_version": c.version, "schema_version": c.schema,
		})
		if err != nil {
			return err
		}
		if !c.wait {
			return printJSON(out)
		}
		summary, _ := out["summary"].(map[string]any)
		id, _ := summary["replay_id"].(string)
		return waitReplay(c.url, id)
	}
	body := map[string]any{"replay_id": c.replayID, "package": c.pkg, "corpus_name": c.name,
		"corpus_version": c.version, "schema_version": c.schema}
	out, err := postRPCTo(c.url, "/registry.v1.Registry/GetReplay", body)
	if err != nil {
		return err
	}
	return printJSON(out)
}

// waitReplay polls because the CLI may be used against a server whose
// background worker is processing the task. A local STORE=memory server
// resumes on the same fixed 500ms interval.
func waitReplay(baseURL, id string) error {
	client := &http.Client{Timeout: 10 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
		raw, _ := json.Marshal(map[string]string{"replay_id": id})
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/registry.v1.Registry/GetReplay", bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var out map[string]any
		if err := json.Unmarshal(data, &out); err != nil {
			return err
		}
		summary, _ := out["summary"].(map[string]any)
		status, _ := summary["status"].(string)
		if status == "COMPLETE" || status == "FAILED" {
			return printJSON(out)
		}
	}
}
