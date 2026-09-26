package registry

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"protocompat/internal/replay"
)

type memReplay struct {
	Replay
	results map[int64]*ReplayItemResult
}

// corpusMemStore adds corpus/replay state to MemStore in the same file's
// type, keeping all in-memory persistence behind one mutex.
func corpusMemState(m *MemStore) {
	if m.corpusSamples == nil {
		m.corpusSamples = map[string]map[string]*CorpusSample{}
		m.corpusSampleIDs = map[int64]*CorpusSample{}
		m.corpusSets = map[string]map[string]map[int]*CorpusSet{}
		m.memberships = map[int64]map[string]*CorpusMembership{}
		m.nextCorpusSampleID, m.nextCorpusSetID = 1, 1
		m.replays = map[string]*memReplay{}
	}
}

func normalizeCorpusInput(in CorpusSampleInput) (CorpusSampleInput, []byte, error) {
	normalized, err := replay.ValidateAndNormalize(replay.Sample{
		Message: in.Message, Encoding: in.Encoding, Data: in.Data, Expected: in.ExpectedResult,
	})
	if err != nil {
		return in, nil, err
	}
	in.Message, in.Encoding, in.Data, in.ExpectedResult = normalized.Message, normalized.Encoding, normalized.Data, normalized.Expected
	fp, summary, err := replay.Fingerprint(replay.Sample{
		Message: in.Message, Encoding: in.Encoding, Data: in.Data, Expected: in.ExpectedResult,
	})
	if err != nil {
		return in, nil, err
	}
	if in.ContentSummary == "" {
		in.ContentSummary = summary
	}
	if in.Key == "" {
		in.Key = hex.EncodeToString(fp)
	}
	return in, fp, nil
}

func (m *MemStore) putCorpusSampleLocked(pkg string, in CorpusSampleInput, fp []byte, now time.Time) *CorpusSample {
	corpusMemState(m)
	byFP := m.corpusSamples[pkg]
	if byFP == nil {
		byFP = map[string]*CorpusSample{}
		m.corpusSamples[pkg] = byFP
	}
	key := hex.EncodeToString(fp)
	if s := byFP[key]; s != nil {
		return s
	}
	s := &CorpusSample{
		ID: m.nextCorpusSampleID, Package: pkg, Fingerprint: append([]byte(nil), fp...),
		Name: in.Name, Message: in.Message, Encoding: in.Encoding, Data: in.Data,
		ExpectedResult: in.ExpectedResult, ContentSummary: in.ContentSummary, CreatedAt: now,
	}
	if s.Name == "" {
		s.Name = in.Key
	}
	byFP[key] = s
	m.corpusSampleIDs[s.ID] = s
	m.nextCorpusSampleID++
	return s
}

func (m *MemStore) applyCorpusMutationLocked(setID int64, pkg string, mutation CorpusMutation, now time.Time) error {
	seen := map[string]bool{}
	for _, in := range mutation.AddOrUpdate {
		normalized, fp, err := normalizeCorpusInput(in)
		if err != nil {
			return err
		}
		if seen[normalized.Key] {
			return fmt.Errorf("%w: duplicate sample key %s in request", ErrCorpusConflict, normalized.Key)
		}
		seen[normalized.Key] = true
		sample := m.putCorpusSampleLocked(pkg, normalized, fp, now)
		name := normalized.Name
		if name == "" {
			name = normalized.Key
		}
		for _, existing := range m.memberships[setID] {
			if existing.SampleID == sample.ID && existing.Key != normalized.Key {
				return fmt.Errorf("%w: identical content is already stored with key %s", ErrCorpusConflict, existing.Key)
			}
		}
		if existing := m.memberships[setID][normalized.Key]; existing != nil && existing.SampleID != sample.ID {
			return fmt.Errorf("%w: sample key already refers to different content", ErrCorpusConflict)
		}
		m.memberships[setID][normalized.Key] = &CorpusMembership{
			CorpusSetID: setID, SampleID: sample.ID, Key: normalized.Key, Name: name, CreatedAt: now,
		}
	}
	for _, key := range mutation.DeleteKeys {
		if seen[key] {
			return fmt.Errorf("%w: key %s is both added and deleted", ErrCorpusConflict, key)
		}
		delete(m.memberships[setID], key)
	}
	return nil
}

func (m *MemStore) CreateCorpusDraft(_ context.Context, pkg, name string, version int, baseVersion *int, mutation CorpusMutation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	sets := m.corpusSets[pkg]
	if sets == nil {
		sets = map[string]map[int]*CorpusSet{}
		m.corpusSets[pkg] = sets
	}
	byName := sets[name]
	if byName == nil {
		byName = map[int]*CorpusSet{}
		sets[name] = byName
	}
	if _, exists := byName[version]; exists {
		return ErrCorpusConflict
	}
	var base *int
	if baseVersion != nil {
		b := *baseVersion
		baseSet, ok := byName[b]
		if !ok {
			return ErrNotFound
		}
		if baseSet.Status != CorpusStatusSealed {
			return ErrSealed
		}
		base = &b
	}
	now := time.Now()
	set := &CorpusSet{ID: m.nextCorpusSetID, Package: pkg, Name: name, Version: version, Status: CorpusStatusDraft, Base: base, CreatedAt: now}
	m.nextCorpusSetID++
	byName[version] = set
	m.memberships[set.ID] = map[string]*CorpusMembership{}
	if base != nil {
		for _, mem := range m.memberships[byName[*base].ID] {
			cp := *mem
			cp.CorpusSetID = set.ID
			cp.CreatedAt = now
			m.memberships[set.ID][cp.Key] = &cp
		}
	}
	if err := m.applyCorpusMutationLocked(set.ID, pkg, mutation, now); err != nil {
		delete(byName, version)
		delete(m.memberships, set.ID)
		return err
	}
	return nil
}

func (m *MemStore) MutateCorpusDraft(_ context.Context, pkg, name string, version int, mutation CorpusMutation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	set, err := m.getCorpusSetLocked(pkg, name, version)
	if err != nil {
		return err
	}
	if set.Status != CorpusStatusDraft {
		return ErrSealed
	}
	return m.applyCorpusMutationLocked(set.ID, pkg, mutation, time.Now())
}

func (m *MemStore) SealCorpusDraft(_ context.Context, pkg, name string, version int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	set, err := m.getCorpusSetLocked(pkg, name, version)
	if err != nil {
		return err
	}
	if set.Status != CorpusStatusDraft {
		return ErrSealed
	}
	if len(m.memberships[set.ID]) == 0 {
		return fmt.Errorf("%w: cannot seal an empty corpus", ErrCorpusConflict)
	}
	now := time.Now()
	set.Status = CorpusStatusSealed
	set.SealedAt = &now
	return nil
}

func (m *MemStore) DeleteCorpusDraft(_ context.Context, pkg, name string, version int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	set, err := m.getCorpusSetLocked(pkg, name, version)
	if err != nil {
		return err
	}
	if set.Status != CorpusStatusDraft {
		return ErrSealed
	}
	delete(m.corpusSets[pkg][name], version)
	delete(m.memberships, set.ID)
	return nil
}

func (m *MemStore) getCorpusSetLocked(pkg, name string, version int) (*CorpusSet, error) {
	if s, ok := m.corpusSets[pkg][name][version]; ok {
		cp := *s
		return &cp, nil
	}
	return nil, ErrNotFound
}

func (m *MemStore) GetCorpusSet(_ context.Context, pkg, name string, version int) (*CorpusSet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	if version == 0 {
		set, err := m.latestCorpusSetLocked(pkg, name)
		if err != nil {
			return nil, err
		}
		cp := *set
		return &cp, nil
	}
	return m.getCorpusSetLocked(pkg, name, version)
}

func (m *MemStore) latestCorpusSetLocked(pkg, name string) (*CorpusSet, error) {
	var latest *CorpusSet
	for _, set := range m.corpusSets[pkg][name] {
		if latest == nil || set.Version > latest.Version {
			latest = set
		}
	}
	if latest == nil {
		return nil, ErrNotFound
	}
	cp := *latest
	return &cp, nil
}

func (m *MemStore) LatestCorpusSet(_ context.Context, pkg, name string) (*CorpusSet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	return m.latestCorpusSetLocked(pkg, name)
}

func (m *MemStore) ListCorpusSets(_ context.Context, pkg, name string) ([]CorpusSet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	var out []CorpusSet
	if name != "" {
		for _, set := range m.corpusSets[pkg][name] {
			out = append(out, *set)
		}
	} else {
		for _, byName := range m.corpusSets[pkg] {
			for _, set := range byName {
				out = append(out, *set)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Version < out[j].Version
	})
	return out, nil
}

func (m *MemStore) ListCorpusSamples(_ context.Context, setID int64) ([]CorpusSampleEnvelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	mems := append([]*CorpusMembership{}, m.memberships[setID]...)
	sort.Slice(mems, func(i, j int) bool { return mems[i].Key < mems[j].Key })
	out := make([]CorpusSampleEnvelope, 0, len(mems))
	for _, mem := range mems {
		s := m.corpusSampleIDs[mem.SampleID]
		out = append(out, CorpusSampleEnvelope{Membership: *mem, Sample: *s})
	}
	return out, nil
}

func (m *MemStore) GetCorpusSample(_ context.Context, _ string, sampleID int64) (*CorpusSample, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	s := m.corpusSampleIDs[sampleID]
	if s == nil {
		return nil, ErrNotFound
	}
	cp := *s
	return &cp, nil
}

func (m *MemStore) StartReplay(_ context.Context, r Replay) (*Replay, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	if existing := m.replays[r.ID]; existing != nil {
		cp := cloneMemReplay(existing)
		return &cp.Replay, false, nil
	}
	var set *CorpusSet
	for _, candidate := range m.corpusSets[r.Package][r.CorpusName] {
		if candidate.Version == r.CorpusVersion {
			set = candidate
		}
	}
	if set == nil || set.Status != CorpusStatusSealed {
		return nil, false, ErrNotFound
	}
	now := time.Now()
	r.Status, r.Total, r.StartedAt, r.UpdatedAt = ReplayStatusPending, len(m.memberships[set.ID]), now, now
	rec := &memReplay{Replay: r, results: map[int64]*ReplayItemResult{}}
	for _, mem := range m.memberships[set.ID] {
		sample := m.corpusSampleIDs[mem.SampleID]
		if sample == nil {
			return nil, false, fmt.Errorf("sample %d missing", mem.SampleID)
		}
		rec.results[mem.SampleID] = &ReplayItemResult{ReplayID: r.ID, SampleID: mem.SampleID, Key: mem.Key, Status: "PENDING", UpdatedAt: now}
	}
	m.replays[r.ID] = rec
	cp := cloneMemReplay(rec)
	return &cp.Replay, true, nil
}

func cloneMemReplay(in *memReplay) memReplay {
	cp := memReplay{Replay: in.Replay, results: map[int64]*ReplayItemResult{}}
	for id, r := range in.results {
		item := *r
		cp.results[id] = &item
	}
	return cp
}

func (m *MemStore) GetReplay(_ context.Context, replayID string) (*Replay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	r := m.replays[replayID]
	if r == nil {
		return nil, ErrNotFound
	}
	cp := r.Replay
	return &cp, nil
}

func (m *MemStore) GetReplayByTarget(_ context.Context, pkg, corpusName, schemaVersion string, corpusVersion int) (*Replay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	for _, r := range m.replays {
		if r.Package == pkg && r.CorpusName == corpusName && r.CorpusVersion == corpusVersion && r.SchemaVersion == schemaVersion {
			cp := r.Replay
			return &cp, nil
		}
	}
	return nil, ErrNotFound
}

func (m *MemStore) ListReplays(_ context.Context, pkg, corpusName string, corpusVersion *int) ([]Replay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	var out []Replay
	for _, r := range m.replays {
		if r.Package != pkg || r.CorpusName != corpusName || corpusVersion != nil && r.CorpusVersion != *corpusVersion {
			continue
		}
		out = append(out, r.Replay)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *MemStore) ListReplayResults(_ context.Context, replayID string) ([]ReplayItemResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	r := m.replays[replayID]
	if r == nil {
		return nil, ErrNotFound
	}
	var ids []int64
	for id := range r.results {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return r.results[ids[i]].Key < r.results[ids[j]].Key })
	out := make([]ReplayItemResult, 0, len(ids))
	for _, id := range ids {
		out = append(out, *r.results[id])
	}
	return out, nil
}

func (m *MemStore) ClaimReplayItem(_ context.Context, replayID string, now, leaseExpires time.Time) (*ClaimedReplay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	r := m.replays[replayID]
	if r == nil {
		return nil, ErrNotFound
	}
	if r.Status != ReplayStatusPending && r.Status != ReplayStatusRunning {
		return nil, ErrNotFound
	}
	var candidate *ReplayItemResult
	for _, res := range r.results {
		if res.Status == "PENDING" || res.Status == "RUNNING" {
			if candidate == nil || res.Key < candidate.Key {
				candidate = res
			}
		}
	}
	if candidate == nil {
		return nil, ErrNotFound
	}
	r.Status, r.LeaseExpiresAt, r.UpdatedAt = ReplayStatusRunning, &leaseExpires, now
	candidate.Status, candidate.UpdatedAt = ReplayStatusRunning, now
	return &ClaimedReplay{Replay: &r.Replay, Item: ReplayItem{SampleID: candidate.SampleID, Key: candidate.Key}}, nil
}

func (m *MemStore) FinishReplayItem(_ context.Context, replayID string, sampleID int64, result replay.Result, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	r := m.replays[replayID]
	if r == nil {
		return ErrNotFound
	}
	item := r.results[sampleID]
	if item == nil {
		return ErrNotFound
	}
	if item.Status == replay.StatusSuccess || item.Status == replay.StatusFailed {
		return nil
	}
	item.Status, item.Error, item.MissingFields, item.JSONDiffs, item.DecodedJSON, item.UpdatedAt =
		result.Status, result.Error, result.MissingFields, result.JSONDiffs, result.DecodedJSON, now
	recomputeMemReplay(r, now)
	return nil
}

func recomputeMemReplay(r *memReplay, now time.Time) {
	r.Succeeded, r.Failed, r.MissingFieldItems, r.JSONDiffItems = 0, 0, 0, 0
	unfinished := 0
	for _, item := range r.results {
		switch item.Status {
		case replay.StatusSuccess:
			r.Succeeded++
		case replay.StatusFailed:
			r.Failed++
		default:
			unfinished++
		}
		if len(item.MissingFields) > 0 {
			r.MissingFieldItems++
		}
		if len(item.JSONDiffs) > 0 {
			r.JSONDiffItems++
		}
	}
	r.UpdatedAt = now
	if unfinished == 0 {
		r.Status, r.LeaseExpiresAt = ReplayStatusComplete, nil
		return
	}
	r.Status = ReplayStatusRunning
}

func (m *MemStore) ListPendingReplays(_ context.Context, before time.Time) ([]Replay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	var out []Replay
	for _, r := range m.replays {
		if r.Status == ReplayStatusPending || r.Status == ReplayStatusRunning && (r.LeaseExpiresAt == nil || r.LeaseExpiresAt.Before(before)) {
			out = append(out, r.Replay)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *MemStore) ReopenReplay(_ context.Context, replayID string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	corpusMemState(m)
	r := m.replays[replayID]
	if r == nil {
		return ErrNotFound
	}
	if r.Status == ReplayStatusRunning {
		r.Status, r.LeaseExpiresAt, r.UpdatedAt = ReplayStatusPending, nil, now
	}
	return nil
}
