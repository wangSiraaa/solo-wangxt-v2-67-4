package registry

import (
	"protocompat/internal/corpus"
)

// --- corpus API types (JSON codec over ConnectRPC) ---

// CorpusUploadSample is a sample submitted with a corpus upload. The
// server computes the content digest; clients never send it.
type CorpusUploadSample struct {
	Name        string              `json:"name,omitempty"`
	Message     string              `json:"message"`
	Encoding    string              `json:"encoding"`
	Data        string              `json:"data"`
	Expectation *corpus.Expectation `json:"expectation,omitempty"`
}

type CreateCorpusRequest struct {
	Package string `json:"package"`
	Corpus  string `json:"corpus"`
	// BaseVersion derives the new draft from a sealed version. 0 means a
	// fresh, empty corpus.
	BaseVersion int                  `json:"base_version,omitempty"`
	Note        string               `json:"note,omitempty"`
	Samples     []CorpusUploadSample `json:"samples,omitempty"`
	// Add/Remove reference already-uploaded samples by digest (incremental
	// corpus construction without resending payloads).
	Add    []string `json:"add,omitempty"`
	Remove []string `json:"remove,omitempty"`
}

type UpdateCorpusRequest struct {
	Package string               `json:"package"`
	Corpus  string               `json:"corpus"`
	Note    string               `json:"note,omitempty"`
	Samples []CorpusUploadSample `json:"samples,omitempty"`
	Add     []string             `json:"add,omitempty"`
	Remove  []string             `json:"remove,omitempty"`
}

type CorpusMeta struct {
	Package     string   `json:"package"`
	Corpus      string   `json:"corpus"`
	Version     int      `json:"version"`
	Status      string   `json:"status"`
	Note        string   `json:"note,omitempty"`
	SampleCount int      `json:"sample_count"`
	Digests     []string `json:"digests,omitempty"`
	CreatedAt   string   `json:"created_at,omitempty"`
	SealedAt    string   `json:"sealed_at,omitempty"`
}

type CorpusResponse struct {
	Corpus CorpusMeta `json:"corpus"`
	// AddedSampleDigests echoes digests of samples first uploaded by this
	// request; AlreadySeen are digests of identical content that already
	// existed (no second sample was created).
	AddedSampleDigests []string `json:"added_sample_digests,omitempty"`
	AlreadySeen        []string `json:"already_seen,omitempty"`
	// AlreadyExisted is true when a draft already existed for this corpus;
	// CreateCorpus then returns that draft untouched (use UpdateCorpus).
	AlreadyExisted bool `json:"already_existed,omitempty"`
}

type SealCorpusRequest struct {
	Package string `json:"package"`
	Corpus  string `json:"corpus"`
}

type GetCorpusRequest struct {
	Package string `json:"package"`
	Corpus  string `json:"corpus"`
	Version int    `json:"version,omitempty"` // 0 = latest
}

type ListCorporaRequest struct {
	Package string `json:"package"`
}

type ListCorporaResponse struct {
	Package string       `json:"package"`
	Corpora []CorpusMeta `json:"corpora"`
}

type DeleteCorpusDraftRequest struct {
	Package string `json:"package"`
	Corpus  string `json:"corpus"`
}

type DeleteCorpusDraftResponse struct {
	Deleted bool `json:"deleted"`
}

// --- replay API types ---

type StartReplayRequest struct {
	Package       string `json:"package"`
	Corpus        string `json:"corpus"`
	CorpusVersion int    `json:"corpus_version"` // sealed set version
	SchemaVersion string `json:"schema_version"`
	// ReplayKey is the client's idempotency key. Empty derives a stable
	// key from (package, corpus, corpus_version, schema_version), so
	// replaying the same corpus against the same schema always returns
	// the same run.
	ReplayKey string `json:"replay_key,omitempty"`
}

type ReplayMeta struct {
	ID            int64           `json:"id"`
	Package       string          `json:"package"`
	ReplayKey     string          `json:"replay_key"`
	Corpus        string          `json:"corpus"`
	CorpusVersion int             `json:"corpus_version"`
	SchemaVersion string          `json:"schema_version"`
	Status        string          `json:"status"`
	Total         int             `json:"total"`
	Completed     int             `json:"completed"`
	Summary       *corpus.Summary `json:"summary,omitempty"`
	CreatedAt     string          `json:"created_at"`
	UpdatedAt     string          `json:"updated_at"`
}

type StartReplayResponse struct {
	Replay ReplayMeta `json:"replay"`
	// AlreadyExisted is true when the replay key already had a run; that
	// run was resumed (if unfinished) rather than duplicated.
	AlreadyExisted bool                `json:"already_existed,omitempty"`
	Items          []corpus.ItemResult `json:"items,omitempty"`
}

type GetReplayRequest struct {
	Package   string `json:"package"`
	ReplayID  int64  `json:"replay_id,omitempty"`
	ReplayKey string `json:"replay_key,omitempty"`
}

type GetReplayResponse struct {
	Replay ReplayMeta          `json:"replay"`
	Items  []corpus.ItemResult `json:"items,omitempty"`
}

type ListReplaysRequest struct {
	Package string `json:"package"`
	Corpus  string `json:"corpus,omitempty"`
}

type ListReplaysResponse struct {
	Package string       `json:"package"`
	Replays []ReplayMeta `json:"replays"`
}

type CompareReplaysRequest struct {
	Package     string `json:"package"`
	OldReplayID int64  `json:"old_replay_id"`
	NewReplayID int64  `json:"new_replay_id"`
}

type CompareReplaysResponse struct {
	Comparison corpus.Comparison `json:"comparison"`
}
