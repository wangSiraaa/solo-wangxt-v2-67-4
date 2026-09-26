// Package replay decodes versioned corpus samples against one registered
// schema version and produces stable, per-item replay evidence.
package replay

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	OutcomeSuccess = "success"
	OutcomeFailure = "failure"

	StatusSuccess = "SUCCESS"
	StatusFailed  = "FAILED"
)

// Sample is one immutable corpus payload.
type Sample struct {
	Message  string         `json:"message"`
	Encoding string         `json:"encoding"`
	Data     string         `json:"data"`
	Expected ExpectedResult `json:"expected_result"`
}

// ExpectedResult describes what the corpus author expects when this sample
// is decoded. ErrorContains is only meaningful for failure expectations.
type ExpectedResult struct {
	Outcome       string `json:"outcome,omitempty"`
	ErrorContains string `json:"error_contains,omitempty"`
	JSON          string `json:"json,omitempty"`
}

// JSONDiff is one stable path through a proto-JSON expectation mismatch.
type JSONDiff struct {
	Path     string `json:"path"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
}

// Result is the replay outcome for one sample.
type Result struct {
	Status        string     `json:"status"`
	Error         string     `json:"error,omitempty"`
	MissingFields []string   `json:"missing_fields,omitempty"`
	JSONDiffs     []JSONDiff `json:"json_diffs,omitempty"`
	DecodedJSON   string     `json:"decoded_json,omitempty"`
}

// ValidateAndNormalize validates payload/expectation syntax independently of
// a schema and fills default expectation values.
func ValidateAndNormalize(in Sample) (Sample, error) {
	in.Message = strings.TrimSpace(in.Message)
	in.Encoding = strings.ToLower(strings.TrimSpace(in.Encoding))
	if in.Message == "" {
		return in, fmt.Errorf("message is required")
	}
	if in.Encoding != "json" && in.Encoding != "wire" {
		return in, fmt.Errorf("encoding must be json or wire")
	}
	switch in.Encoding {
	case "json":
		if !json.Valid([]byte(in.Data)) {
			return in, fmt.Errorf("data is not valid JSON")
		}
	case "wire":
		if _, err := base64.StdEncoding.DecodeString(in.Data); err != nil {
			return in, fmt.Errorf("wire data is not valid base64: %w", err)
		}
	}
	if in.Expected.Outcome == "" {
		in.Expected.Outcome = OutcomeSuccess
	}
	if in.Expected.Outcome != OutcomeSuccess && in.Expected.Outcome != OutcomeFailure {
		return in, fmt.Errorf("expected_result.outcome must be success or failure")
	}
	if in.Expected.ErrorContains != "" && in.Expected.Outcome != OutcomeFailure {
		return in, fmt.Errorf("error_contains requires expected_result.outcome=failure")
	}
	if in.Expected.JSON != "" {
		if !json.Valid([]byte(in.Expected.JSON)) {
			return in, fmt.Errorf("expected_result.json is not valid JSON")
		}
	}
	return in, nil
}

// Decode runs one sample against a linked descriptor set.
func Decode(files *protoregistry.Files, in Sample) Result {
	in, err := ValidateAndNormalize(in)
	if err != nil {
		return Result{Status: StatusFailed, Error: err.Error()}
	}
	desc, err := files.FindDescriptorByName(protoreflect.FullName(in.Message))
	if err != nil {
		return Result{Status: StatusFailed, Error: fmt.Sprintf("message %s not found in schema", in.Message)}
	}
	md, ok := desc.(protoreflect.MessageDescriptor)
	if !ok || md.IsMapEntry() {
		return Result{Status: StatusFailed, Error: fmt.Sprintf("%s is not a message", in.Message)}
	}

	payload, err := payloadBytes(in)
	if err != nil {
		return Result{Status: StatusFailed, Error: err.Error()}
	}
	msg := dynamicpb.NewMessage(md)
	decodeErr := unmarshal(in.Encoding, payload, msg, false)

	if in.Expected.Outcome == OutcomeFailure {
		return expectedFailureResult(decodeErr, in.Expected)
	}
	if decodeErr != nil {
		res := Result{Status: StatusFailed, Error: decodeErr.Error()}
		// protojson rejects unknown fields. Recover the known-field projection
		// so the report can still name the fields the new schema cannot carry.
		if in.Encoding == "json" {
			var raw any
			if jerr := json.Unmarshal(payload, &raw); jerr == nil {
				res.MissingFields = missingJSONFields(md, raw, "")
			} else {
				return Result{Status: StatusFailed, Error: "invalid JSON payload: " + jerr.Error()}
			}
			known := dynamicpb.NewMessage(md)
			if lerr := unmarshal("json", payload, known, true); lerr == nil {
				if b, merr := canonicalJSON(known); merr == nil {
					res.DecodedJSON = b
				}
				res.JSONDiffs = jsonBehaviorDiffs(raw, known)
			}
		}
		return res
	}

	res := Result{Status: StatusSuccess}
	switch in.Encoding {
	case "wire":
		res.MissingFields = missingWireFields(msg, "")
	case "json":
		var raw any
		if err := json.Unmarshal(payload, &raw); err != nil {
			return Result{Status: StatusFailed, Error: "invalid JSON payload: " + err.Error()}
		}
		res.MissingFields = missingJSONFields(md, raw, "")
		res.JSONDiffs = jsonBehaviorDiffs(raw, msg)
	}
	if b, err := canonicalJSON(msg); err == nil {
		res.DecodedJSON = b
	}
	if in.Expected.JSON != "" {
		diffs, err := jsonExpectationDiffs(md, in.Expected.JSON, msg)
		if err != nil {
			return Result{Status: StatusFailed, Error: "invalid expected JSON: " + err.Error()}
		}
		res.JSONDiffs = diffs
		if len(diffs) > 0 {
			res.Status = StatusFailed
		}
	}
	return res
}

func expectedFailureResult(decodeErr error, expected ExpectedResult) Result {
	if decodeErr == nil {
		return Result{Status: StatusFailed, Error: "expected decode to fail but it succeeded"}
	}
	if expected.ErrorContains != "" && !strings.Contains(decodeErr.Error(), expected.ErrorContains) {
		return Result{Status: StatusFailed, Error: fmt.Sprintf("decode failed as expected but error %q does not contain %q", decodeErr.Error(), expected.ErrorContains)}
	}
	return Result{Status: StatusSuccess, Error: decodeErr.Error()}
}

func payloadBytes(in Sample) ([]byte, error) {
	if in.Encoding == "json" {
		return []byte(in.Data), nil
	}
	b, err := base64.StdEncoding.DecodeString(in.Data)
	if err != nil {
		return nil, fmt.Errorf("wire sample is not valid base64: %w", err)
	}
	return b, nil
}

func unmarshal(encoding string, payload []byte, m proto.Message, discardUnknown bool) error {
	if encoding == "json" {
		return protojson.UnmarshalOptions{AllowPartial: true, DiscardUnknown: discardUnknown}.Unmarshal(payload, m)
	}
	return proto.UnmarshalOptions{AllowPartial: true}.Unmarshal(payload, m)
}

func canonicalJSON(m proto.Message) (string, error) {
	b, err := protojson.MarshalOptions{AllowPartial: true}.Marshal(m)
	return string(b), err
}

func jsonExpectationDiffs(md protoreflect.MessageDescriptor, expectedText string, actual proto.Message) ([]JSONDiff, error) {
	expectedMsg := dynamicpb.NewMessage(md)
	if err := protojson.UnmarshalOptions{AllowPartial: true}.Unmarshal([]byte(expectedText), expectedMsg); err != nil {
		return nil, err
	}
	expectedCanonical, err := canonicalJSON(expectedMsg)
	if err != nil {
		return nil, err
	}
	actualCanonical, err := canonicalJSON(actual)
	if err != nil {
		return nil, err
	}
	if expectedCanonical == actualCanonical {
		return nil, nil
	}
	var expectedGeneric, actualGeneric any
	if err := json.Unmarshal([]byte(expectedCanonical), &expectedGeneric); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(actualCanonical), &actualGeneric); err != nil {
		return nil, err
	}
	var diffs []JSONDiff
	diffGenericValues(expectedGeneric, actualGeneric, "", &diffs)
	if len(diffs) == 0 {
		diffs = []JSONDiff{{Path: "", Expected: expectedCanonical, Actual: actualCanonical}}
	}
	return diffs, nil
}

func diffGenericValues(expected, actual any, path string, diffs *[]JSONDiff) {
	em, eok := expected.(map[string]any)
	am, aok := actual.(map[string]any)
	if eok && aok {
		keys := map[string]bool{}
		for k := range em {
			keys[k] = true
		}
		for k := range am {
			keys[k] = true
		}
		ordered := make([]string, 0, len(keys))
		for k := range keys {
			ordered = append(ordered, k)
		}
		sort.Strings(ordered)
		for _, k := range ordered {
			diffGenericValues(em[k], am[k], joinPath(path, k), diffs)
		}
		return
	}
	ea, eok := expected.([]any)
	aa, aok := actual.([]any)
	if eok && aok {
		n := len(ea)
		if len(aa) > n {
			n = len(aa)
		}
		for i := 0; i < n; i++ {
			p := fmt.Sprintf("%s[%d]", path, i)
			switch {
			case i >= len(ea):
				*diffs = append(*diffs, JSONDiff{Path: p, Expected: "<absent>", Actual: formatGeneric(aa[i])})
			case i >= len(aa):
				*diffs = append(*diffs, JSONDiff{Path: p, Expected: formatGeneric(ea[i]), Actual: "<absent>"})
			default:
				diffGenericValues(ea[i], aa[i], p, diffs)
			}
		}
		return
	}
	if formatGeneric(expected) != formatGeneric(actual) {
		*diffs = append(*diffs, JSONDiff{Path: path, Expected: formatGeneric(expected), Actual: formatGeneric(actual)})
	}
}

func formatGeneric(v any) string {
	if v == nil {
		return "null"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

func jsonBehaviorDiffs(raw any, actual proto.Message) []JSONDiff {
	canonical, err := canonicalJSON(actual)
	if err != nil {
		return nil
	}
	var actualGeneric any
	if err := json.Unmarshal([]byte(canonical), &actualGeneric); err != nil {
		return nil
	}
	md := actual.ProtoReflect().Descriptor()
	projected := projectJSONBehavior(md, raw)
	var diffs []JSONDiff
	diffGenericValues(projected, actualGeneric, "", &diffs)
	return diffs
}

func projectJSONBehavior(md protoreflect.MessageDescriptor, v any) any {
	obj, ok := v.(map[string]any)
	if !ok {
		return v
	}
	out := map[string]any{}
	for key, val := range obj {
		fd := findJSONField(md, key)
		if fd == nil {
			continue // unknown fields are reported separately as missing fields
		}
		switch {
		case fd.IsMap():
			childMap, ok := val.(map[string]any)
			if !ok {
				out[key] = val
				continue
			}
			projectedMap := map[string]any{}
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				for mk, child := range childMap {
					projectedMap[mk] = projectJSONBehavior(fd.MapValue().Message(), child)
				}
			} else {
				for mk, child := range childMap {
					projectedMap[mk] = child
				}
			}
			out[key] = projectedMap
		case fd.IsList():
			arr, ok := val.([]any)
			if !ok {
				out[key] = val
				continue
			}
			if fd.Kind() == protoreflect.MessageKind {
				for i, child := range arr {
					arr[i] = projectJSONBehavior(fd.Message(), child)
				}
			}
			out[key] = arr
		case fd.Kind() == protoreflect.MessageKind:
			out[key] = projectJSONBehavior(fd.Message(), val)
		default:
			out[key] = val
		}
	}
	return out
}

func missingWireFields(m protoreflect.Message, path string) []string {
	var missing []string
	collectUnknownFields(m.GetUnknown(), path, &missing)
	collectUnknownInKnownFields(m, path, &missing)
	sort.Strings(missing)
	return uniqueStrings(missing)
}

func collectUnknownFields(b []byte, path string, missing *[]string) {
	for len(b) > 0 {
		num, wt, n := protowire.ConsumeTag(b)
		if n < 0 {
			return
		}
		_, valLen := protowire.ConsumeFieldValue(num, wt, b)
		if valLen < 0 {
			return
		}
		*missing = append(*missing, fmt.Sprintf("%s#%d", pathPrefix(path), num))
		b = b[n+valLen:]
	}
}

func collectUnknownInKnownFields(m protoreflect.Message, path string, missing *[]string) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap():
			v.Map().Range(func(k protoreflect.MapKey, mv protoreflect.Value) bool {
				if fd.MapValue().Kind() == protoreflect.MessageKind || fd.MapValue().Kind() == protoreflect.GroupKind {
					p := fmt.Sprintf("%s[%v]", joinPath(path, string(fd.Name())), k.Interface())
					*missing = append(*missing, missingWireFields(mv.Message(), p)...)
				}
				return true
			})
		case fd.IsList():
			if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
				l := v.List()
				for i := 0; i < l.Len(); i++ {
					p := fmt.Sprintf("%s[%d]", joinPath(path, string(fd.Name())), i)
					*missing = append(*missing, missingWireFields(l.Get(i).Message(), p)...)
				}
			}
		case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
			*missing = append(*missing, missingWireFields(v.Message(), joinPath(path, string(fd.Name())))...)
		}
		return true
	})
}

func missingJSONFields(md protoreflect.MessageDescriptor, raw any, path string) []string {
	obj, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	var missing []string
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fd := findJSONField(md, key)
		if fd == nil {
			missing = append(missing, joinPath(path, key))
			continue
		}
		childPath := joinPath(path, string(fd.Name()))
		switch {
		case fd.IsMap():
			childMap, ok := obj[key].(map[string]any)
			if !ok || fd.MapValue().Kind() != protoreflect.MessageKind {
				continue
			}
			mapKeys := make([]string, 0, len(childMap))
			for mk := range childMap {
				mapKeys = append(mapKeys, mk)
			}
			sort.Strings(mapKeys)
			for _, mk := range mapKeys {
				missing = append(missing, missingJSONFields(fd.MapValue().Message(), childMap[mk], fmt.Sprintf("%s[%s]", childPath, mk))...)
			}
		case fd.IsList():
			arr, ok := obj[key].([]any)
			if !ok || fd.Kind() != protoreflect.MessageKind {
				continue
			}
			for i, child := range arr {
				missing = append(missing, missingJSONFields(fd.Message(), child, fmt.Sprintf("%s[%d]", childPath, i))...)
			}
		case fd.Kind() == protoreflect.MessageKind:
			missing = append(missing, missingJSONFields(fd.Message(), obj[key], childPath)...)
		}
	}
	sort.Strings(missing)
	return uniqueStrings(missing)
}

func findJSONField(md protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
	if fd := md.Fields().ByJSONName(name); fd != nil {
		return fd
	}
	return md.Fields().ByName(protoreflect.Name(name))
}

// Fingerprint returns the canonical content hash used to deduplicate corpus
// samples. Semantically equivalent JSON is treated as identical; wire samples
// are identified by their decoded bytes.
func Fingerprint(in Sample) ([]byte, string, error) {
	in, err := ValidateAndNormalize(in)
	if err != nil {
		return nil, "", err
	}
	var payloadKey []byte
	var summary string
	switch in.Encoding {
	case "json":
		var generic any
		decoder := json.NewDecoder(strings.NewReader(in.Data))
		decoder.UseNumber()
		if err := decoder.Decode(&generic); err != nil {
			return nil, "", err
		}
		payloadKey, err = json.Marshal(generic)
		if err != nil {
			return nil, "", err
		}
		top, _ := generic.(map[string]any)
		summary = describeJSON(top, len(payloadKey))
	case "wire":
		payloadKey, _ = base64.StdEncoding.DecodeString(in.Data)
		summary = describeWire(payloadKey)
	}
	expectedCanonical, err := json.Marshal(in.Expected)
	if err != nil {
		return nil, "", err
	}
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00", in.Message, in.Encoding)
	h.Write([]byte{0})
	h.Write(payloadKey)
	h.Write([]byte{0})
	h.Write(expectedCanonical)
	sum := h.Sum(nil)
	summary = fmt.Sprintf("%s; sha256=%s; expected=%s", summary, hex.EncodeToString(sum[:8]), in.Expected.Outcome)
	return sum, summary, nil
}

func describeJSON(top map[string]any, size int) string {
	keys := make([]string, 0, len(top))
	for k := range top {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return fmt.Sprintf("json:%d bytes; fields=%s", size, strings.Join(keys, ","))
}

func describeWire(wire []byte) string {
	original := wire
	seen := map[protowire.Number]bool{}
	for len(wire) > 0 {
		num, _, n := protowire.ConsumeTag(wire)
		if n < 0 {
			break
		}
		seen[num] = true
		wire = wire[n:]
	}
	nums := make([]int, 0, len(seen))
	for n := range seen {
		nums = append(nums, int(n))
	}
	sort.Ints(nums)
	parts := make([]string, len(nums))
	for i, n := range nums {
		parts[i] = fmt.Sprint(n)
	}
	return fmt.Sprintf("wire:%d bytes; fields=%s", len(original), strings.Join(parts, ","))
}

func joinPath(base, elem string) string {
	if base == "" {
		return elem
	}
	return base + "." + elem
}

func pathPrefix(path string) string {
	if path == "" {
		return ""
	}
	return path + "."
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	out := in[:0]
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
