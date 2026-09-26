package registry

import (
	"context"
	"errors"
	"time"

	"protocompat/internal/corpus"
)

// Corpus set lifecycle states.
const (
	CorpusDraft   = "draft"
	CorpusSealed  = "sealed"
	CorpusDeleted = "deleted"
)

// Replay run states.
const (
	ReplayRunning   = "running"
	ReplayCompleted = "completed"
	ReplayFailed    = "failed"
)

// Item claim states (replay_items.status).
const (
	ItemPending   = "pending"
	ItemClaimed   = "claimed"
	ItemCompleted = "completed"
)

// ErrSealed is returned when an operation tries to mutate a sealed corpus
// version. Sealed versions are immutable.
var ErrSealed = errors.New("corpus version is sealed and immutable")

// ErrNoDraft is returned when updating/deleting a corpus that has no open
// draft.
var ErrNoDraft = errors.New("no draft corpus version to update")

// ErrNoChange is returned when an upload would produce a version identical
// to its base (no samples added or removed).
var ErrNoChange = errors.New("new corpus version changes nothing over its base")

// ErrDraftExists is returned by CreateDraft when an unsealed draft is
// already open for the corpus (the caller should update that draft).
var ErrDraftExists = errors.New("a draft corpus version already exists")

// ErrItemAlreadyComplete is returned when completing a replay item that
// already has a result: the same input key must produce one result only.
var ErrItemAlreadyComplete = errors.New("replay item already completed")

// ErrReplayExists is returned by CreateReplay when the idempotency key
// already maps to a run. The existing run is attached to the error so
// callers can resume/return it.
var ErrReplayExists = errors.New("replay with this key already exists")

// CorpusSampleRecord is one content-addressed sample owned by a package.
type CorpusSampleRecord struct {
	Digest      string
	Name        string
	Message     string
	Encoding    string
	Data        string
	Expectation *corpus.Expectation
	CreatedAt   time.Time
}

// CorpusSet is one version of a named corpus.
type CorpusSet struct {
	ID          int64
	Package     string
	Corpus      string
	Version     int
	Status      string
	Note        string
	CreatedAt   time.Time
	SealedAt    *time.Time
	Digests     []string // membership, in position order (Get/List populate)
	SampleCount int
}

// Replay is one replay run.
type Replay struct {
	ID            int64
	Package       string
	Key           string
	Corpus        string
	CorpusVersion int
	SetID         int64
	SchemaVersion string
	Status        string
	Total         int
	Completed     int
	Summary       *corpus.Summary
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// ReplayItem is one claimed or completed item of a run.
type ReplayItem struct {
	Position  int
	Digest    string
	Sample    corpus.Sample
	Status    string
	Attempt   int
	Result    *corpus.ItemResult
	ClaimedAt *time.Time
	UpdatedAt time.Time
}

// ReplayExistenceError wraps ErrReplayExists with the pre-existing run,
// the same way a conflict surfaces an existing version.
type ReplayExistenceError struct {
	Replay *Replay
}

func (e *ReplayExistenceError) Error() string { return ErrReplayExists.Error() }
func (e *ReplayExistenceError) Unwrap() error { return ErrReplayExists }

// FullStore combines the schema registry and corpus persistence surfaces.
// PGStore and MemStore both implement it.
type FullStore interface {
	Store
	CorpusStore
}

// CorpusStore contains the corpus/replay persistence operations. It is a
// separate interface from Store so the two surfaces can evolve
// independently; PGStore and MemStore implement both.
type CorpusStore interface {
	// PutSample stores a content-addressed sample. Returns created=false
	// when the digest already exists for the package (re-uploading
	// identical content never creates a new sample).
	PutSample(ctx context.Context, pkg string, rec CorpusSampleRecord) (created bool, err error)
	GetSample(ctx context.Context, pkg, digest string) (*CorpusSampleRecord, error)

	// CreateDraft creates a new draft version of a named corpus derived
	// from baseVersion (0 = empty). add/remove are sample digests.
	CreateDraft(ctx context.Context, pkg, name string, baseVersion int, note string, add, remove []string) (*CorpusSet, bool, error)
	// UpdateDraft replaces an existing draft's membership and note.
	UpdateDraft(ctx context.Context, pkg, name, note string, add, remove []string) (*CorpusSet, error)
	GetCorpusSet(ctx context.Context, pkg, name string, version int) (*CorpusSet, error)
	GetDraft(ctx context.Context, pkg, name string) (*CorpusSet, error)
	LatestCorpusVersion(ctx context.Context, pkg, name string) (*CorpusSet, error)
	ListCorpora(ctx context.Context, pkg string) ([]CorpusSet, error)
	SealCorpus(ctx context.Context, pkg, name string) (*CorpusSet, error)
	DeleteDraft(ctx context.Context, pkg, name string) error

	// --- replays ---

	// CreateReplay inserts a run with its items snapshotted from a sealed
	// set. Returns ReplayExistenceError when key already exists.
	CreateReplay(ctx context.Context, r Replay, items []ReplayItem) (*Replay, error)
	GetReplay(ctx context.Context, pkg string, id int64) (*Replay, error)
	GetReplayByKey(ctx context.Context, pkg, key string) (*Replay, error)
	ListReplays(ctx context.Context, pkg, corpus string) ([]Replay, error)
	// ClaimNextItem atomically takes one pending item, or an expired
	// claimed item whose lease predates claimBefore (crash recovery).
	// Returns nil when no work remains.
	ClaimNextItem(ctx context.Context, replayID int64, claimBefore time.Time) (*ReplayItem, error)
	// CompleteItem records a result and marks the item completed. Fails
	// if the item is already completed — the same input key must produce
	// exactly one result.
	CompleteItem(ctx context.Context, replayID int64, position int, result corpus.ItemResult) error
	// CompleteReplay sets status/summary; must be called after all items
	// are complete.
	CompleteReplay(ctx context.Context, replayID int64, status string, summary corpus.Summary) error
	ListReplayItems(ctx context.Context, replayID int64) ([]ReplayItem, error)
}
