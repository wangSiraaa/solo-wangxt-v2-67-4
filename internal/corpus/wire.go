package corpus

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

func payloadBytes(s Sample) ([]byte, error) {
	if s.Encoding == EncodingJSON {
		return []byte(s.Data), nil
	}
	return decodeBase64(s.Data)
}

func decodeBase64(data string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(data)
}

func findMessage(files *protoregistry.Files, name protoreflect.FullName) protoreflect.MessageDescriptor {
	d, err := files.FindDescriptorByName(name)
	if err != nil {
		return nil
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok || md.IsMapEntry() {
		return nil
	}
	return md
}

func unmarshalPayload(encoding string, payload []byte, md protoreflect.MessageDescriptor) (protoreflect.Message, error) {
	m := dynamicpb.NewMessage(md)
	if encoding == EncodingJSON {
		// Strict: unknown JSON keys and malformed values are failures.
		err := protojson.UnmarshalOptions{AllowPartial: true, DiscardUnknown: false}.Unmarshal(payload, m)
		if err != nil {
			return nil, err
		}
		return m, nil
	}
	if err := (proto.UnmarshalOptions{AllowPartial: true}).Unmarshal(payload, m); err != nil {
		return nil, err
	}
	return m, nil
}

func canonicalJSON(m protoreflect.Message) string {
	b, err := protojson.MarshalOptions{AllowPartial: true}.Marshal(m.Interface())
	if err != nil {
		return ""
	}
	return string(b)
}

func deterministicWireHex(m protoreflect.Message) string {
	b, err := proto.MarshalOptions{Deterministic: true, AllowPartial: true}.Marshal(m.Interface())
	if err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

func compactJSON(raw []byte) ([]byte, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func genericJSON(raw []byte) (any, error) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// probeWireUnknowns parses wire bytes and walks the resulting unknown-field
// buffers (including inside message fields and list elements), naming each
// unrecognized field by number as "#<num>" at its dotted/indexed path.
func probeWireUnknowns(md protoreflect.MessageDescriptor, payload []byte) []string {
	m := dynamicpb.NewMessage(md)
	if err := (proto.UnmarshalOptions{AllowPartial: true, DiscardUnknown: false}).Unmarshal(payload, m); err != nil {
		return nil
	}
	var lost []string
	walkUnknowns(m, string(md.FullName()), &lost)
	return lost
}

func walkUnknowns(msg protoreflect.Message, path string, lost *[]string) {
	unknownNums(msg.GetUnknown(), path, lost)
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap():
			v.Map().Range(func(k protoreflect.MapKey, mv protoreflect.Value) bool {
				if fd.MapValue().Kind() == protoreflect.MessageKind {
					p := fmt.Sprintf("%s[%v]", joinPath(path, string(fd.Name())), k.Interface())
					walkUnknowns(mv.Message(), p, lost)
				}
				return true
			})
		case fd.IsList():
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				if fd.Kind() == protoreflect.MessageKind {
					walkUnknowns(l.Get(i).Message(), fmt.Sprintf("%s[%d]", joinPath(path, string(fd.Name())), i), lost)
				}
			}
		case fd.Kind() == protoreflect.MessageKind:
			walkUnknowns(v.Message(), joinPath(path, string(fd.Name())), lost)
		}
		return true
	})
}

// unknownNums reads field tags out of a raw unknown-field buffer without
// needing the wire package's public types.
func unknownNums(raw []byte, path string, lost *[]string) {
	for len(raw) > 0 {
		tag, m := consumeVarint(raw)
		if m == 0 {
			return
		}
		fieldNum := protoreflect.FieldNumber(tag >> 3)
		wireType := int(tag & 0x7)
		var valLen int
		var ok bool
		switch wireType {
		case 0: // varint
			_, valLen, ok = consumeVarintOK(raw[m:])
		case 1: // fixed64
			valLen = 8
			ok = len(raw[m:]) >= 8
		case 2: // length-delimited
			v, l, vok := consumeVarintOK(raw[m:])
			valLen = l + int(v)
			ok = vok && len(raw[m+l:]) >= int(v)
		case 5: // fixed32
			valLen = 4
			ok = len(raw[m:]) >= 4
		default: // group start (3,4) unsupported here
			*lost = append(*lost, fmt.Sprintf("%s.#%d", path, fieldNum))
			return
		}
		if !ok {
			return
		}
		*lost = append(*lost, fmt.Sprintf("%s.#%d", path, fieldNum))
		raw = raw[m+valLen:]
	}
}

// probeJSONUnknowns unmarshals into a generic JSON value and walks it
// alongside the descriptor, recording keys the schema does not name.
func probeJSONUnknowns(md protoreflect.MessageDescriptor, raw []byte) []string {
	v, err := genericJSON(raw)
	if err != nil {
		return nil
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	var lost []string
	walkJSONUnknowns(md, obj, string(md.FullName()), &lost)
	return lost
}

func walkJSONUnknowns(md protoreflect.MessageDescriptor, obj map[string]any, path string, lost *[]string) {
	for key, val := range obj {
		fd := jsonFieldByName(md, key)
		if fd == nil {
			*lost = append(*lost, joinPath(path, key))
			continue
		}
		switch {
		case fd.IsMap():
			if m, ok := val.(map[string]any); ok {
				if fd.MapValue().Kind() == protoreflect.MessageKind {
					for k, mv := range m {
						if child, ok := mv.(map[string]any); ok {
							walkJSONUnknowns(fd.MapValue().Message(), child,
								fmt.Sprintf("%s[%s]", joinPath(path, key), k), lost)
						}
					}
				}
			}
		case fd.IsList():
			if arr, ok := val.([]any); ok {
				if fd.Kind() == protoreflect.MessageKind {
					for i, el := range arr {
						if child, ok := el.(map[string]any); ok {
							walkJSONUnknowns(fd.Message(), child,
								fmt.Sprintf("%s[%d]", joinPath(path, key), i), lost)
						}
					}
				}
			}
		case fd.Kind() == protoreflect.MessageKind:
			if child, ok := val.(map[string]any); ok {
				walkJSONUnknowns(fd.Message(), child, joinPath(path, key), lost)
			}
		}
	}
}

// jsonFieldByName accepts both the proto field name and its lowerCamelCase
// JSON name (protojson accepts both on input).
func jsonFieldByName(md protoreflect.MessageDescriptor, key string) protoreflect.FieldDescriptor {
	if fd := md.Fields().ByName(protoreflect.Name(key)); fd != nil {
		return fd
	}
	return md.Fields().ByJSONName(key)
}

func joinPath(base, elem string) string {
	if base == "" {
		return elem
	}
	return base + "." + elem
}

// --- minimal varint reader (avoids depending on internals) ---

func consumeVarint(b []byte) (uint64, int) {
	v, n, ok := consumeVarintOK(b)
	if !ok {
		return 0, 0
	}
	return v, n
}

func consumeVarintOK(b []byte) (uint64, int, bool) {
	var x uint64
	for i := 0; i < len(b) && i < 10; i++ {
		c := b[i]
		x |= uint64(c&0x7f) << (7 * i)
		if c < 0x80 {
			return x, i + 1, true
		}
	}
	return 0, 0, false
}
