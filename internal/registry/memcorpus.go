package registry

import (
	"context"
	"fmt"
	"sort"
	"time"

	"protocompat/internal/corpus"
)

// memCorpusState holds the corpus/replay side of MemStore behind the same
// mutex as the schema side.
type memCorpusState struct {
	sampleSeq  int64
	samples    map[string]map[string]*CorpusSampleRecord // pkg -> digest -> rec
	setSeq     int64
	sets       []*CorpusSet
	setSamples map[int64][]string // set id -> ordered digests
	replaySeq  int64
	replays    map[int64]*Replay
	replayKeys map[string]int64 // pkg + "/" + key -> id
	items      map[int64][]*ReplayItem
}

func (m *MemStore) corpus() *memCorpusState {
	if m.c != nil {
		return m.c
	}
	m.c = &memCorpusState{
		samples:    map[string]map[string]*CorpusSampleRecord{},
		setSamples: map[int64][]string{},
		replays:    map[int64]*Replay{},
		replayKeys: map[string]int64{},
		items:      map[int64][]*ReplayItem{},
	}
	return m.c
}

func (m *MemStore) PutSample(_ context.Context, pkg string, rec CorpusSampleRecord) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.corpus()
	byDigest, ok := c.samples[pkg]
	if !ok {
		byDigest = map[string]*CorpusSampleRecord{}
		c.samples[pkg] = byDigest
	}
	if _, exists := byDigest[rec.Digest]; exists {
		return false, nil // identical content: never a second sample
	}
	c.sampleSeq++
	cp := rec
	cp.CreatedAt = time.Now()
	byDigest[rec.Digest] = &cp
	return true, nil
}

func (m *MemStore) GetSample(_ context.Context, pkg, digest string) (*CorpusSampleRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.corpus()
	if rec, ok := c.samples[pkg][digest]; ok {
		cp := *rec
		return &cp, nil
	}
	return nil, ErrNotFound
}

func (m *MemStore) CreateDraft(_ context.Context, pkg, name string, baseVersion int, note string, add, remove []string) (*CorpusSet, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.corpus()

	latest := m.latestSetLocked(pkg, name)
	if latest != nil && latest.Status == CorpusDraft {
		cp := *latest
		return &cp, false, ErrDraftExists
	}

	var membership []string
	if baseVersion > 0 {
		base := m.findSetLocked(pkg, name, baseVersion)
		if base == nil {
			return nil, false, ErrNotFound
		}
		if base.Status != CorpusSealed {
			return nil, false, fmt.Errorf("base version %d must be sealed before deriving a new version", baseVersion)
		}
		membership = append(membership, c.setSamples[base.ID]...)
	}

	next, err := m.applyChangesLocked(pkg, membership, add, remove)
	if err != nil {
		return nil, false, err
	}
	if baseVersion > 0 && len(add) == 0 && len(remove) == 0 {
		return nil, false, ErrNoChange
	}

	nextVersion := 1
	if latest != nil {
		nextVersion = latest.Version + 1
	}
	c.setSeq++
	set := &CorpusSet{
		ID: c.setSeq, Package: pkg, Corpus: name, Version: nextVersion,
		Status: CorpusDraft, Note: note, CreatedAt: time.Now(),
	}
	c.sets = append(c.sets, set)
	c.setSamples[set.ID] = append([]string(nil), next...)
	set.Digests = append([]string(nil), next...)
	set.SampleCount = len(next)
	cp := *set
	return &cp, true, nil
}

func (m *MemStore) UpdateDraft(_ context.Context, pkg, name, note string, add, remove []string) (*CorpusSet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	latest := m.latestSetLocked(pkg, name)
	if latest == nil {
		return nil, ErrNoDraft
	}
	if latest.Status == CorpusSealed {
		return nil, ErrSealed
	}
	if latest.Status == CorpusDeleted {
		return nil, ErrNoDraft
	}
	c := m.corpus()
	next, err := m.applyChangesLocked(pkg, c.setSamples[latest.ID], add, remove)
	if err != nil {
		return nil, err
	}
	c.setSamples[latest.ID] = append([]string(nil), next...)
	latest.Note = note
	latest.Digests = append([]string(nil), next...)
	latest.SampleCount = len(next)
	cp := *latest
	return &cp, nil
}

// applyChangesLocked resolves add/remove digests against stored samples.
// Caller must hold m.mu.
func (m *MemStore) applyChangesLocked(pkg string, membership, add, remove []string) ([]string, error) {
	c := m.corpus()
	byDigest := c.samples[pkg]
	out := append([]string(nil), membership...)
	present := map[string]bool{}
	for _, d := range out {
		present[d] = true
	}
	removed := map[string]bool{}
	for _, d := range remove {
		removed[d] = true
	}
	filtered := out[:0]
	for _, d := range out {
		if !removed[d] {
			filtered = append(filtered, d)
		} else {
			delete(present, d)
		}
	}
	out = filtered
	for _, d := range add {
		if byDigest == nil || byDigest[d] == nil {
			return nil, fmt.Errorf("sample %s: %w", d, ErrNotFound)
		}
		if !present[d] {
			present[d] = true
			out = append(out, d)
		}
	}
	return out, nil
}

func (m *MemStore) SealCorpus(_ context.Context, pkg, name string) (*CorpusSet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.corpus()
	latest := m.latestSetLocked(pkg, name)
	if latest == nil {
		return nil, ErrNoDraft
	}
	if latest.Status == CorpusSealed {
		return nil, ErrSealed
	}
	if latest.Status == CorpusDeleted {
		return nil, ErrNoDraft
	}
	if len(c.setSamples[latest.ID]) == 0 {
		return nil, fmt.Errorf("cannot seal an empty corpus")
	}
	latest.Status = CorpusSealed
	t := time.Now()
	latest.SealedAt = &t
	cp := *latest
	return &cp, nil
}

func (m *MemStore) DeleteDraft(_ context.Context, pkg, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	latest := m.latestSetLocked(pkg, name)
	if latest == nil || latest.Status != CorpusDraft {
		return ErrNoDraft
	}
	latest.Status = CorpusDeleted
	return nil
}

func (m *MemStore) GetCorpusSet(_ context.Context, pkg, name string, version int) (*CorpusSet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := m.findSetLocked(pkg, name, version)
	if set == nil {
		return nil, ErrNotFound
	}
	return m.copySetLocked(set), nil
}

func (m *MemStore) GetDraft(_ context.Context, pkg, name string) (*CorpusSet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	latest := m.latestSetLocked(pkg, name)
	if latest == nil || latest.Status != CorpusDraft {
		return nil, ErrNotFound
	}
	return m.copySetLocked(latest), nil
}

func (m *MemStore) LatestCorpusVersion(_ context.Context, pkg, name string) (*CorpusSet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var best *CorpusSet
	for _, set := range m.corpus().sets {
		if set.Package != pkg || set.Corpus != name || set.Status == CorpusDeleted {
			continue
		}
		if best == nil || set.Version > best.Version {
			best = set
		}
	}
	if best == nil {
		return nil, ErrNotFound
	}
	return m.copySetLocked(best), nil
}

func (m *MemStore) ListCorpora(_ context.Context, pkg string) ([]CorpusSet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	latestByName := map[string]*CorpusSet{}
	for _, set := range m.corpus().sets {
		if set.Package != pkg || set.Status == CorpusDeleted {
			continue
		}
		cur := latestByName[set.Corpus]
		if cur == nil || set.Version > cur.Version {
			latestByName[set.Corpus] = set
		}
	}
	names := make([]string, 0, len(latestByName))
	for n := range latestByName {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]CorpusSet, 0, len(names))
	for _, n := range names {
		out = append(out, *m.copySetLocked(latestByName[n]))
	}
	return out, nil
}

// --- replays ---

func (m *MemStore) CreateReplay(_ context.Context, r Replay, items []ReplayItem) (*Replay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.corpus()
	keyIndex := r.Package + "/" + r.Key
	if id, ok := c.replayKeys[keyIndex]; ok {
		cp := *c.replays[id]
		return &cp, &ReplayExistenceError{Replay: &cp}
	}
	set := m.findSetByIDLocked(r.SetID)
	if set == nil {
		return nil, ErrNotFound
	}
	if set.Status != CorpusSealed {
		return nil, fmt.Errorf("replays can only target a sealed corpus set")
	}
	c.replaySeq++
	id := c.replaySeq
	now := time.Now()
	run := &Replay{
		ID: id, Package: r.Package, Key: r.Key, Corpus: r.Corpus,
		CorpusVersion: r.CorpusVersion, SetID: r.SetID,
		SchemaVersion: r.SchemaVersion, Status: ReplayRunning,
		Total: len(items), CreatedAt: now, UpdatedAt: now,
	}
	c.replays[id] = run
	c.replayKeys[keyIndex] = id
	copied := make([]*ReplayItem, len(items))
	for i, it := range items {
		cp := it
		cp.Position = i
		cp.Status = ItemPending
		cp.UpdatedAt = now
		copied[i] = &cp
	}
	c.items[id] = copied
	out := *run
	return &out, nil
}

func (m *MemStore) ClaimNextItem(_ context.Context, replayID int64, claimBefore time.Time) (*ReplayItem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := m.corpus().items[replayID]
	if items == nil {
		return nil, ErrNotFound
	}
	for _, it := range items {
		switch it.Status {
		case ItemPending:
			it.Status = ItemClaimed
			it.Attempt++
			t := time.Now()
			it.ClaimedAt = &t
			it.UpdatedAt = t
			cp := *it
			return &cp, nil
		case ItemClaimed:
			// Crash recovery: an expired lease from a dead worker is
			// reclaimable.
			if it.ClaimedAt != nil && it.ClaimedAt.Before(claimBefore) {
				it.Attempt++
				t := time.Now()
				it.ClaimedAt = &t
				it.UpdatedAt = t
				cp := *it
				return &cp, nil
			}
		}
	}
	return nil, nil
}

func (m *MemStore) CompleteItem(_ context.Context, replayID int64, position int, result corpus.ItemResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.corpus()
	items := c.items[replayID]
	if items == nil {
		return ErrNotFound
	}
	if position < 0 || position >= len(items) {
		return ErrNotFound
	}
	it := items[position]
	if it.Status == ItemCompleted {
		// Same input key already produced a result: never overwrite.
		return ErrItemAlreadyComplete
	}
	res := result
	it.Result = &res
	it.Status = ItemCompleted
	it.UpdatedAt = time.Now()
	run := c.replays[replayID]
	run.Completed++
	run.UpdatedAt = time.Now()
	return nil
}

func (m *MemStore) CompleteReplay(_ context.Context, replayID int64, status string, summary corpus.Summary) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	run := m.corpus().replays[replayID]
	if run == nil {
		return ErrNotFound
	}
	if run.Status != ReplayRunning && run.Status != ReplayFailed {
		return ErrNotFound
	}
	s := summary
	run.Status = status
	run.Summary = &s
	run.UpdatedAt = time.Now()
	return nil
}

func (m *MemStore) GetReplay(_ context.Context, pkg string, id int64) (*Replay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run := m.corpus().replays[id]
	if run == nil || run.Package != pkg {
		return nil, ErrNotFound
	}
	cp := *run
	return &cp, nil
}

func (m *MemStore) GetReplayByKey(_ context.Context, pkg, key string) (*Replay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.corpus()
	id, ok := c.replayKeys[pkg+"/"+key]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *c.replays[id]
	return &cp, nil
}

func (m *MemStore) ListReplays(_ context.Context, pkg, corpusName string) ([]Replay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.corpus()
	ids := make([]int64, 0, len(c.replays))
	for id := range c.replays {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var out []Replay
	for _, id := range ids {
		r := c.replays[id]
		if r.Package != pkg || (corpusName != "" && r.Corpus != corpusName) {
			continue
		}
		out = append(out, *r)
	}
	return out, nil
}

func (m *MemStore) ListReplayItems(_ context.Context, replayID int64) ([]ReplayItem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := m.corpus().items[replayID]
	if items == nil {
		return nil, ErrNotFound
	}
	out := make([]ReplayItem, 0, len(items))
	for _, it := range items {
		out = append(out, *it)
	}
	return out, nil
}

// --- helpers (caller holds the lock) ---

func (m *MemStore) latestSetLocked(pkg, name string) *CorpusSet {
	var latest *CorpusSet
	for _, set := range m.corpus().sets {
		if set.Package == pkg && set.Corpus == name {
			if latest == nil || set.Version > latest.Version {
				latest = set
			}
		}
	}
	return latest
}

func (m *MemStore) findSetLocked(pkg, name string, version int) *CorpusSet {
	for _, set := range m.corpus().sets {
		if set.Package == pkg && set.Corpus == name && set.Version == version {
			return set
		}
	}
	return nil
}

func (m *MemStore) findSetByIDLocked(id int64) *CorpusSet {
	for _, set := range m.corpus().sets {
		if set.ID == id {
			return set
		}
	}
	return nil
}

func (m *MemStore) copySetLocked(set *CorpusSet) *CorpusSet {
	cp := *set
	cp.Digests = append([]string(nil), m.corpus().setSamples[set.ID]...)
	cp.SampleCount = len(cp.Digests)
	return &cp
}
