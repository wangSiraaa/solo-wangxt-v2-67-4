package registry

import (
	"context"
	"errors"
	"time"

	"protocompat/internal/replay"
)

var (
	// ErrSealed is returned when a sealed corpus version is targeted by a
	// mutating operation.
	ErrSealed = errors.New("corpus version is sealed and immutable")
	// ErrCorpusConflict is returned when a caller-supplied sample key already
	// exists with different sample content, or a draft version already exists.
	ErrCorpusConflict = errors.New("corpus conflict")
)

const (
	CorpusStatusDraft    = "DRAFT"
	CorpusStatusSealed   = "SEALED"
	ReplayStatusPending  = "PENDING"
	ReplayStatusRunning  = "RUNNING"
	ReplayStatusComplete = "COMPLETE"
	ReplayStatusFailed   = "FAILED"
)

// CorpusSampleInput is a create/update payload from an API client.
type CorpusSampleInput struct {
	Key            string `json:"key,omitempty"`
	Name           string `json:"name,omitempty"`
	Message        string `json:"message"`
	Encoding       string `json:"encoding"`
	Data           string `json:"data"`
	ExpectedResult replay.ExpectedResult `json:"expected_result,omitempty"`
	ContentSummary string `json:"content_summary,omitempty"`
}

// CorpusSample is a content-addressed payload within one proto package.
type CorpusSample struct {
	ID             int64
	Package        string
	Fingerprint    []byte
	Name           string
	Message        string
	Encoding       string
	Data           string
	ExpectedResult replay.ExpectedResult
	ContentSummary string
	CreatedAt      time.Time
}

// CorpusSet is one named, versioned collection of samples.
type CorpusSet struct {
	ID        int64
	Package   string
	Name      string
	Version   int
	Status    string
	Base      *int
	CreatedAt time.Time
	SealedAt  *time.Time
}

// CorpusMembership associates a sample with one draft/sealed corpus version.
type CorpusMembership struct {
	CorpusSetID int64
	SampleID    int64
	Key         string
	Name        string
	CreatedAt   time.Time
}

type CorpusSampleEnvelope struct {
	Membership CorpusMembership
	Sample     CorpusSample
}

type CorpusMutation struct {
	AddOrUpdate []CorpusSampleInput `json:"add_or_update,omitempty"`
	DeleteKeys  []string            `json:"delete_keys,omitempty"`
}

// Replay is one idempotent replay of a sealed corpus against one schema.
type Replay struct {
	ID                 string
	Package            string
	CorpusName         string
	CorpusVersion      int
	SchemaVersion      string
	Status             string
	Total              int
	Succeeded          int
	Failed             int
	MissingFieldItems  int
	JSONDiffItems      int
	StartedAt          time.Time
	UpdatedAt          time.Time
	LeaseExpiresAt     *time.Time
	LastUnclaimedError string
}

type ClaimedReplay struct {
	Replay *Replay
	Item   ReplayItem
}

// ReplayItem identifies the work to process without denormalizing payloads.
type ReplayItem struct {
	SampleID int64
	Key      string
}

// ReplayItemResult is stored per (replay, sample).
type ReplayItemResult struct {
	ReplayID      string
	SampleID      int64
	Key           string
	Status        string
	Error         string
	MissingFields []string
	JSONDiffs     []replay.JSONDiff
	DecodedJSON   string
	UpdatedAt     time.Time
}

// CorpusStore contains persistence methods used by corpus/replay workflows.
type CorpusStore interface {
	CreateCorpusDraft(ctx context.Context, pkg, name string, version int, baseVersion *int, mutation CorpusMutation) error
	MutateCorpusDraft(ctx context.Context, pkg, name string, version int, mutation CorpusMutation) error
	SealCorpusDraft(ctx context.Context, pkg, name string, version int) error
	DeleteCorpusDraft(ctx context.Context, pkg, name string, version int) error
	GetCorpusSet(ctx context.Context, pkg, name string, version int) (*CorpusSet, error)
	LatestCorpusSet(ctx context.Context, pkg, name string) (*CorpusSet, error)
	ListCorpusSets(ctx context.Context, pkg, name string) ([]CorpusSet, error)
	ListCorpusSamples(ctx context.Context, setID int64) ([]CorpusSampleEnvelope, error)
	GetCorpusSample(ctx context.Context, pkg string, sampleID int64) (*CorpusSample, error)

	StartReplay(ctx context.Context, r Replay) (*Replay, bool, error)
	GetReplay(ctx context.Context, replayID string) (*Replay, error)
	GetReplayByTarget(ctx context.Context, pkg, corpusName, schemaVersion string, corpusVersion int) (*Replay, error)
	ListReplays(ctx context.Context, pkg, corpusName string, corpusVersion *int) ([]Replay, error)
	ListReplayResults(ctx context.Context, replayID string) ([]ReplayItemResult, error)
	ClaimReplayItem(ctx context.Context, replayID string, now, leaseExpires time.Time) (*ClaimedReplay, error)
	FinishReplayItem(ctx context.Context, replayID string, sampleID int64, result replay.Result, now time.Time) error
	ListPendingReplays(ctx context.Context, before time.Time) ([]Replay, error)
	ReopenReplay(ctx context.Context, replayID string, now time.Time) error
}
