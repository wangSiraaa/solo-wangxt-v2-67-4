// Package corpus decodes versioned sample payloads against a single schema
// version and summarizes the outcome, so a sealed corpus can be replayed
// against old and new schemas and the results compared.
//
// Unlike compat (which parses one payload under two schemas), a replay
// decodes every corpus item under exactly one chosen schema version. Each
// item independently reports decode failure, field loss (data the schema
// no longer names), JSON-mapping behavior differences, and whether an
// uploaded expectation held. One bad item never aborts the run.
package corpus

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Encoding marks a sample's on-the-wire form.
const (
	EncodingJSON = "json"
	EncodingWire = "wire"
)

// Sample is one corpus item: the original payload plus the result the
// submitter expects when it is replayed.
type Sample struct {
	Name        string       `json:"name,omitempty"`
	Message     string       `json:"message"` // fully-qualified message name
	Encoding    string       `json:"encoding"`
	Data        string       `json:"data"` // JSON text, or base64 wire bytes
	Expectation *Expectation `json:"expectation,omitempty"`
}

// Expectation is the submitter's claim about a replay outcome. Every field
// is optional; only provided fields are checked.
type Expectation struct {
	// Status is "ok" (the payload must decode) or "decode_error" (it must
	// not). Empty means no expectation about the decode outcome.
	Status string `json:"status,omitempty"`
	// JSON is the expected canonical protojson rendering of the decoded
	// message. It is compared structurally, so key order and whitespace
	// do not matter.
	JSON string `json:"json,omitempty"`
}

// Item statuses (primary classification; the boolean flags carry the full
// picture because an item can be both field-lost and json-different).
const (
	StatusOK                  = "OK"
	StatusDecodeFailed        = "DECODE_FAILED"
	StatusFieldLost           = "FIELD_LOST"
	StatusJSONDiff            = "JSON_DIFF"
	StatusExpectationMismatch = "EXPECTATION_MISMATCH"
)

// Diff is one structural difference between two JSON renderings.
type Diff struct {
	Path string `json:"path"`
	Old  string `json:"old"`
	New  string `json:"new"`
}

// ItemResult is the fully self-contained outcome of replaying one sample.
// It gets snapshotted into replay_items, so historical replays stay
// readable after the draft corpus that produced them is deleted.
type ItemResult struct {
	Digest   string `json:"digest"`
	Name     string `json:"name,omitempty"`
	Message  string `json:"message"`
	Encoding string `json:"encoding"`

	// DecodeOK is false when the payload cannot be parsed under the
	// chosen schema version.
	DecodeOK bool `json:"decode_ok"`
	// Status is the primary, single-word classification (see constants).
	Status string `json:"status"`

	// LostFields lists field paths present in the payload that the
	// schema cannot name anymore (unknown wire numbers / unknown JSON
	// keys), e.g. "acme.M.lines[2].#5".
	LostFields []string `json:"lost_fields,omitempty"`
	// JSONDiffs compares behavior between the submitted JSON and the
	// canonical protojson rendering (JSON samples), or between the
	// expectation's JSON and the rendering (wire samples that carry one).
	JSONDiffs []Diff `json:"json_diffs,omitempty"`

	// ExpectationMatched is nil when no expectation was supplied.
	ExpectationMatched *bool `json:"expectation_matched,omitempty"`

	// Canonical forms, kept for traceability and cross-replay diffs.
	WireHex       string `json:"wire_hex,omitempty"`
	CanonicalJSON string `json:"canonical_json,omitempty"`

	Error string `json:"error,omitempty"`
}

// PrimaryStatus derives the single-word status from the detailed flags.
// Decode failure dominates; otherwise field loss, then JSON behavior, then
// a broken expectation.
func PrimaryStatus(decodeOK bool, lost, jsonDiffs int, hasExpectation, expectationMatched bool) string {
	switch {
	case !decodeOK:
		return StatusDecodeFailed
	case lost > 0:
		return StatusFieldLost
	case jsonDiffs > 0:
		return StatusJSONDiff
	case hasExpectation && !expectationMatched:
		return StatusExpectationMismatch
	default:
		return StatusOK
	}
}

// Decode replays one sample under the given schema closure. It never
// returns an error for payload-level problems: those are reported in the
// ItemResult so a batch run continues with the remaining items. The only
// hard failure is a malformed submission (bad base64), which is likewise
// reported on the item rather than returned.
func Decode(files *protoregistry.Files, digest string, s Sample) (res ItemResult) {
	res = ItemResult{
		Digest: digest, Name: s.Name, Message: s.Message, Encoding: s.Encoding,
	}
	// Expectation evaluation and primary status are derived on exactly one
	// exit path so even a decode failure is checked against an expected
	// "decode_error" claim.
	defer func() { res.finish(s.Expectation) }()

	if s.Encoding != EncodingJSON && s.Encoding != EncodingWire {
		res.Error = `unknown encoding (want "json" or "wire")`
		return res
	}

	payload, err := payloadBytes(s)
	if err != nil {
		res.Error = err.Error()
		return res
	}

	md := findMessage(files, protoreflect.FullName(s.Message))
	if md == nil {
		res.Error = "message " + s.Message + " not found in this schema version"
		// Field loss against a missing message is not attributable; leave
		// LostFields empty (nothing to walk without a descriptor).
		return res
	}

	// Field-loss probing is independent from strict decoding and works
	// even when the strict parse refuses the payload (JSON unknown keys
	// make protojson fail outright; wire unknown fields parse leniently
	// and land in the unknown-field buffer).
	switch s.Encoding {
	case EncodingWire:
		res.LostFields = probeWireUnknowns(md, payload)
	case EncodingJSON:
		res.LostFields = probeJSONUnknowns(md, []byte(s.Data))
	}

	decoded, derr := unmarshalPayload(s.Encoding, payload, md)
	if derr != nil {
		res.Error = derr.Error()
		return res
	}
	res.DecodeOK = true

	res.WireHex = deterministicWireHex(decoded)
	res.CanonicalJSON = canonicalJSON(decoded)

	switch s.Encoding {
	case EncodingJSON:
		// JSON behavior: does the submitted JSON survive a round-trip
		// through canonical protojson unchanged (structurally)? Enum
		// number->name, int64 quoting, unknown-key dropping, etc. show
		// up here as path-level diffs.
		if raw, perr := genericJSON([]byte(s.Data)); perr == nil {
			if canon, perr := genericJSON([]byte(res.CanonicalJSON)); perr == nil {
				res.JSONDiffs = DiffJSON(canon, raw, s.Message)
			}
		}
	case EncodingWire:
		// A wire payload carries no JSON of its own; an expected JSON
		// rendering (if supplied) is the only JSON-behavior evidence.
		if s.Expectation != nil && s.Expectation.JSON != "" {
			res.JSONDiffs = compareExpectedJSON(s.Expectation.JSON, res.CanonicalJSON, s.Message)
		}
	}
	return res
}

// finish evaluates the expectation once the other dimensions are known and
// derives the primary status.
func (r *ItemResult) finish(exp *Expectation) {
	matched := true
	if exp != nil && (exp.Status != "" || exp.JSON != "") {
		switch exp.Status {
		case "":
			// no opinion on decode outcome
		case "ok":
			if !r.DecodeOK {
				matched = false
			}
		case "decode_error":
			if r.DecodeOK {
				matched = false
			}
		default:
			matched = false
		}
		if exp.JSON != "" {
			// Missing/extra JSON diffs against the expectation are
			// expectation failures, not ambient JSON-behavior diffs.
			if diffs := compareExpectedJSON(exp.JSON, r.CanonicalJSON, r.Message); len(diffs) > 0 {
				matched = false
			}
		}
		r.ExpectationMatched = &matched
	}
	hasExpectation := exp != nil && (exp.Status != "" || exp.JSON != "")
	r.Status = PrimaryStatus(r.DecodeOK, len(r.LostFields), len(r.JSONDiffs), hasExpectation, matched)
}

// DigestOf is the stable content identity of a sample. JSON payloads are
// compacted first, so formatting differences never create a second sample
// for the same content.
func DigestOf(s Sample) (string, error) {
	data := s.Data
	if s.Encoding == EncodingJSON && s.Data != "" {
		if compact, err := compactJSON([]byte(s.Data)); err == nil {
			data = string(compact)
		} else {
			return "", err
		}
	}
	canon := struct {
		Message     string       `json:"message"`
		Encoding    string       `json:"encoding"`
		Data        string       `json:"data"`
		Expectation *Expectation `json:"expectation,omitempty"`
	}{Message: s.Message, Encoding: s.Encoding, Data: data, Expectation: s.Expectation}
	b, err := json.Marshal(canon)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// ValidateUpload checks a submission before it enters a corpus draft.
func ValidateUpload(s Sample) error {
	if s.Message == "" {
		return errInvalid("sample requires a message name")
	}
	if s.Name != "" && len(s.Name) > 200 {
		return errInvalid("sample name too long")
	}
	switch s.Encoding {
	case EncodingJSON:
		if s.Data == "" {
			return errInvalid("json sample data is empty")
		}
		if _, err := compactJSON([]byte(s.Data)); err != nil {
			return errInvalid("json sample data is not valid JSON: " + err.Error())
		}
	case EncodingWire:
		if _, err := decodeBase64(s.Data); err != nil {
			return errInvalid("wire sample data is not valid base64: " + err.Error())
		}
	default:
		return errInvalid(`encoding must be "json" or "wire"`)
	}
	if s.Expectation != nil {
		switch s.Expectation.Status {
		case "", "ok", "decode_error":
		default:
			return errInvalid(`expectation.status must be "ok" or "decode_error"`)
		}
		if s.Expectation.JSON != "" {
			if _, err := compactJSON([]byte(s.Expectation.JSON)); err != nil {
				return errInvalid("expectation.json is not valid JSON: " + err.Error())
			}
		}
	}
	return nil
}

type validationError struct{ msg string }

func (e *validationError) Error() string { return e.msg }

func errInvalid(msg string) error { return &validationError{msg: msg} }

// IsValidationError reports whether err came from ValidateUpload.
func IsValidationError(err error) bool {
	_, ok := err.(*validationError)
	return ok
}
