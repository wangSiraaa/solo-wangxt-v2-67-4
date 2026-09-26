package corpus

import (
	"fmt"
	"sort"
	"strings"
)

// DiffJSON produces a deterministic, path-addressed list of structural
// differences between two generic JSON values. Numbers compare by their
// json.Number text so that 5 and "5" are detected as different kinds of
// value (relevant for the int64 JSON mapping). At most maxDiffs paths are
// returned.
func DiffJSON(a, b any, root string) []Diff {
	var diffs []Diff
	diffValue(a, b, root, &diffs)
	return diffs
}

const maxDiffs = 64

func diffValue(a, b any, path string, diffs *[]Diff) {
	if len(*diffs) >= maxDiffs {
		return
	}
	am, aok := a.(map[string]any)
	bm, bok := b.(map[string]any)
	if aok && bok {
		diffObject(am, bm, path, diffs)
		return
	}
	aa, aok := a.([]any)
	ba, bok := b.([]any)
	if aok || bok {
		if !aok || !bok {
			add(diffs, path, a, b)
			return
		}
		diffArray(aa, ba, path, diffs)
		return
	}
	if !scalarEqual(a, b) {
		add(diffs, path, a, b)
	}
}

func diffObject(a, b map[string]any, path string, diffs *[]Diff) {
	keys := make(map[string]bool, len(a)+len(b))
	for k := range a {
		keys[k] = true
	}
	for k := range b {
		keys[k] = true
	}
	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, k)
	}
	sort.Strings(ordered)
	for _, k := range ordered {
		av, ina := a[k]
		bv, inb := b[k]
		p := path + "." + k
		switch {
		case ina && !inb:
			add(diffs, p, av, absent)
		case !ina && inb:
			add(diffs, p, absent, bv)
		default:
			diffValue(av, bv, p, diffs)
		}
		if len(*diffs) >= maxDiffs {
			return
		}
	}
}

func diffArray(a, b []any, path string, diffs *[]Diff) {
	n := len(a)
	if len(b) > n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("%s[%d]", path, i)
		switch {
		case i >= len(a):
			add(diffs, p, absent, b[i])
		case i >= len(b):
			add(diffs, p, a[i], absent)
		default:
			diffValue(a[i], b[i], p, diffs)
		}
		if len(*diffs) >= maxDiffs {
			return
		}
	}
}

func scalarEqual(a, b any) bool {
	// json.Number preserves the textual form: 5 != "5", 5 != 5.0.
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b) && fmt.Sprintf("%T", a) == fmt.Sprintf("%T", b)
}

const absent = "<absent>"

func add(diffs *[]Diff, path string, a, b any) {
	*diffs = append(*diffs, Diff{Path: path, Old: formatScalar(a), New: formatScalar(b)})
}

func formatScalar(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return fmt.Sprintf("%q", x)
	case map[string]any:
		return "{...}"
	case []any:
		return fmt.Sprintf("[%d items]", len(x))
	default:
		return fmt.Sprintf("%v", v)
	}
}

// compareExpectedJSON structurally compares an expectation against the
// canonical rendering. Both sides are normalized through JSON first.
func compareExpectedJSON(expected, canonical, root string) []Diff {
	ev, err := genericJSON([]byte(expected))
	if err != nil {
		return []Diff{{Path: root, Old: "<invalid expectation JSON>", New: err.Error()}}
	}
	cv, err := genericJSON([]byte(canonical))
	if err != nil {
		return []Diff{{Path: root, Old: "<unrenderable canonical JSON>", New: err.Error()}}
	}
	return DiffJSON(ev, cv, root)
}

// RenderDiffs formats diffs for CLI text output.
func RenderDiffs(diffs []Diff) string {
	var b strings.Builder
	for _, d := range diffs {
		fmt.Fprintf(&b, "    %s: %s -> %s\n", d.Path, d.Old, d.New)
	}
	return b.String()
}
