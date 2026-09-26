package corpus

import "sort"

// Summary is the aggregate outcome of replaying a whole corpus set.
type Summary struct {
	Total                 int `json:"total"`
	OK                    int `json:"ok"`
	DecodeFailed          int `json:"decode_failed"`
	FieldLost             int `json:"field_lost"`
	JSONDiffs             int `json:"json_diffs"`
	ExpectationMismatched int `json:"expectation_mismatched"`
	// items with lost field paths
	ItemsWithLostFields int `json:"items_with_lost_fields"`
}

// Summarize aggregates item results into a stable summary.
func Summarize(results []ItemResult) Summary {
	var s Summary
	s.Total = len(results)
	for _, r := range results {
		if !r.DecodeOK {
			s.DecodeFailed++
		}
		if len(r.LostFields) > 0 {
			s.FieldLost++
			s.ItemsWithLostFields++
		}
		if len(r.JSONDiffs) > 0 {
			s.JSONDiffs++
		}
		if r.ExpectationMatched != nil && !*r.ExpectationMatched {
			s.ExpectationMismatched++
		}
		if r.Status == StatusOK {
			s.OK++
		}
	}
	return s
}

// --- cross-replay comparison -------------------------------------------

// ItemComparison aligns one sample's outcome across two replays of the
// same corpus (typically old vs new schema).
type ItemComparison struct {
	Digest   string      `json:"digest"`
	Name     string      `json:"name,omitempty"`
	Message  string      `json:"message"`
	Encoding string      `json:"encoding"`
	Changed  bool        `json:"changed"`
	Old      *ItemResult `json:"old"`
	New      *ItemResult `json:"new"`
	// Regressed marks a change where the new schema replay is strictly
	// worse: it newly fails to decode, newly loses fields, or grows new
	// JSON differences.
	Regressed bool `json:"regressed"`
	// Diffs are field paths that appeared (new) or disappeared (fixed),
	// drawn from lost fields and JSON diff paths on both sides.
	PathsAdded   []string `json:"paths_added,omitempty"`
	PathsRemoved []string `json:"paths_removed,omitempty"`
}

// ComparisonSummary totals the changes between two replays.
type ComparisonSummary struct {
	TotalItems          int `json:"total_items"`
	ChangedItems        int `json:"changed_items"`
	RegressedItems      int `json:"regressed_items"`
	FixedItems          int `json:"fixed_items"`
	NewDecodeFailures   int `json:"new_decode_failures"`
	FixedDecodeFailures int `json:"fixed_decode_failures"`
	NewLostFieldPaths   int `json:"new_lost_field_paths"`
	FixedLostFieldPaths int `json:"fixed_lost_field_paths"`
	NewJSONDiffPaths    int `json:"new_json_diff_paths"`
	FixedJSONDiffPaths  int `json:"fixed_json_diff_paths"`
}

// Comparison is the stable, traceable difference between two replays.
type Comparison struct {
	OldReplayID string            `json:"old_replay_id"`
	NewReplayID string            `json:"new_replay_id"`
	OldSchema   string            `json:"old_schema_version"`
	NewSchema   string            `json:"new_schema_version"`
	Summary     ComparisonSummary `json:"summary"`
	Items       []ItemComparison  `json:"items"`
}

// CompareReplays aligns two replays by sample digest. Only completed items
// participate; unfinished items are reported nowhere (a comparison of
// in-flight runs is meaningless). Results are deterministic: items sorted
// by digest, paths sorted and deduplicated.
func CompareReplays(oldSchemaVersion, newSchemaVersion string, oldID, newID string, oldItems, newItems []ItemResult) Comparison {
	oldByDigest := map[string]ItemResult{}
	newByDigest := map[string]ItemResult{}
	for _, r := range oldItems {
		oldByDigest[r.Digest] = r
	}
	for _, r := range newItems {
		newByDigest[r.Digest] = r
	}
	digests := make(map[string]bool, len(oldByDigest)+len(newByDigest))
	for d := range oldByDigest {
		digests[d] = true
	}
	for d := range newByDigest {
		digests[d] = true
	}
	ordered := make([]string, 0, len(digests))
	for d := range digests {
		ordered = append(ordered, d)
	}
	sort.Strings(ordered)

	out := Comparison{OldReplayID: oldID, NewReplayID: newID, OldSchema: oldSchemaVersion, NewSchema: newSchemaVersion}
	out.Summary.TotalItems = len(ordered)
	for _, d := range ordered {
		o, oOK := oldByDigest[d]
		n, nOK := newByDigest[d]
		if !oOK || !nOK {
			// A digest missing on one side means the corpus composition
			// differs; replays of the same sealed corpus always share the
			// full digest set, but guard anyway by treating it as changed.
			ic := ItemComparison{Digest: d, Changed: true, Regressed: !nOK}
			if oOK {
				oo := o
				ic.Old = &oo
				ic.Name, ic.Message, ic.Encoding = o.Name, o.Message, o.Encoding
			}
			if nOK {
				nn := n
				ic.New = &nn
				ic.Name, ic.Message, ic.Encoding = n.Name, n.Message, n.Encoding
			}
			out.Items = append(out.Items, ic)
			out.Summary.ChangedItems++
			if ic.Regressed {
				out.Summary.RegressedItems++
			} else {
				out.Summary.FixedItems++
			}
			continue
		}

		oldPaths := problemPaths(o)
		newPaths := problemPaths(n)
		added := sortedDifference(newPaths, oldPaths)
		removed := sortedDifference(oldPaths, newPaths)
		changed := (!o.DecodeOK) != (!n.DecodeOK) || len(added) != 0 || len(removed) != 0 ||
			o.Status != n.Status || o.CanonicalJSON != n.CanonicalJSON || o.WireHex != n.WireHex
		// Regression: the new schema fails where the old one decoded, or
		// at least one field path newly became a problem.
		regressed := changed && ((!n.DecodeOK && o.DecodeOK) || len(added) > 0)

		oo, nn := o, n
		ic := ItemComparison{
			Digest: d, Name: n.Name, Message: n.Message, Encoding: n.Encoding,
			Changed: changed, Old: &oo, New: &nn, Regressed: regressed,
			PathsAdded: added, PathsRemoved: removed,
		}
		out.Items = append(out.Items, ic)
		if changed {
			out.Summary.ChangedItems++
			if regressed {
				out.Summary.RegressedItems++
			} else if len(removed) > 0 || (!o.DecodeOK && n.DecodeOK) {
				out.Summary.FixedItems++
			}
		}
		if !n.DecodeOK && o.DecodeOK {
			out.Summary.NewDecodeFailures++
		}
		if !o.DecodeOK && n.DecodeOK {
			out.Summary.FixedDecodeFailures++
		}
		out.Summary.NewLostFieldPaths += countLostAdded(o, n)
		out.Summary.FixedLostFieldPaths += countLostRemoved(o, n)
		out.Summary.NewJSONDiffPaths += countJSONAdded(o, n)
		out.Summary.FixedJSONDiffPaths += countJSONRemoved(o, n)
	}
	return out
}

// problemPaths is the set of field paths flagged on one item: lost fields
// plus every JSON diff path. Decode failure is represented implicitly
// (there are no paths when the message is missing).
func problemPaths(r ItemResult) map[string]bool {
	paths := map[string]bool{}
	for _, p := range r.LostFields {
		paths[p] = true
	}
	for _, d := range r.JSONDiffs {
		paths[d.Path] = true
	}
	return paths
}

func sortedDifference(a, b map[string]bool) []string {
	var out []string
	for p := range a {
		if !b[p] {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func countLostAdded(o, n ItemResult) int { return setDiffSize(n.lostSet(), o.lostSet()) }
func countLostRemoved(o, n ItemResult) int {
	return setDiffSize(o.lostSet(), n.lostSet())
}
func countJSONAdded(o, n ItemResult) int   { return setDiffSize(n.jsonSet(), o.jsonSet()) }
func countJSONRemoved(o, n ItemResult) int { return setDiffSize(o.jsonSet(), n.jsonSet()) }

func (r ItemResult) lostSet() map[string]bool {
	s := map[string]bool{}
	for _, p := range r.LostFields {
		s[p] = true
	}
	return s
}

func (r ItemResult) jsonSet() map[string]bool {
	s := map[string]bool{}
	for _, d := range r.JSONDiffs {
		s[d.Path] = true
	}
	return s
}

func setDiffSize(a, b map[string]bool) int {
	n := 0
	for p := range a {
		if !b[p] {
			n++
		}
	}
	return n
}
