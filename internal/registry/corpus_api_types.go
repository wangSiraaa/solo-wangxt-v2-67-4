package registry

import "protocompat/internal/replay"

// --- Versioned corpus API types ---

type CorpusSampleView struct {
	Key             string                `json:"key"`
	Name            string                `json:"name,omitempty"`
	Message         string                `json:"message"`
	Encoding        string                `json:"encoding"`
	Data            string                `json:"data"`
	ExpectedResult  replay.ExpectedResult `json:"expected_result,omitempty"`
	ContentSummary  string                `json:"content_summary"`
	Fingerprint     string                `json:"fingerprint"`
	CreatedAt       string                `json:"created_at,omitempty"`
}

type CorpusSetMeta struct {
	Name       string `json:"name"`
	Version    int    `json:"version"`
	Status     string `json:"status"`
	Base       *int   `json:"base_version,omitempty"`
	Total      int    `json:"total"`
	CreatedAt  string `json:"created_at"`
	SealedAt   string `json:"sealed_at,omitempty"`
}

type CreateCorpusSetRequest struct {
	Package     string         `json:"package"`
	Name        string         `json:"name"`
	Version     int            `json:"version,omitempty"`
	BaseVersion int            `json:"base_version,omitempty"` // 0 means latest; negative means no base
	AddOrUpdate []CorpusSampleInput `json:"add_or_update,omitempty"`
	DeleteKeys  []string       `json:"delete_keys,omitempty"`
}

type UpdateCorpusSetRequest struct {
	Package     string         `json:"package"`
	Name        string         `json:"name"`
	Version     int            `json:"version"`
	AddOrUpdate []CorpusSampleInput `json:"add_or_update,omitempty"`
	DeleteKeys  []string       `json:"delete_keys,omitempty"`
}

type SealCorpusSetRequest struct {
	Package string `json:"package"`
	Name    string `json:"name"`
	Version int    `json:"version"`
}

type DeleteCorpusDraftRequest struct {
	Package string `json:"package"`
	Name    string `json:"name"`
	Version int    `json:"version"`
}

type DeleteCorpusDraftResponse struct {
	Deleted bool `json:"deleted"`
}

type GetCorpusSetRequest struct {
	Package string `json:"package"`
	Name    string `json:"name"`
	Version int    `json:"version,omitempty"` // 0 = latest
}

type ListCorpusSetsRequest struct {
	Package string `json:"package"`
	Name    string `json:"name,omitempty"`
}

type CorpusSetResponse struct {
	Meta    CorpusSetMeta   `json:"meta"`
	Samples []CorpusSampleView  `json:"samples,omitempty"`
}

type ListCorpusSetsResponse struct {
	Package string          `json:"package"`
	Sets    []CorpusSetMeta `json:"sets"`
}

type StartReplayRequest struct {
	Package       string `json:"package"`
	CorpusName    string `json:"corpus_name"`
	CorpusVersion int    `json:"corpus_version,omitempty"` // 0 = latest sealed
	SchemaVersion string `json:"schema_version"`
}

type GetReplayRequest struct {
	ReplayID string `json:"replay_id,omitempty"`

	Package       string `json:"package,omitempty"`
	CorpusName    string `json:"corpus_name,omitempty"`
	CorpusVersion int    `json:"corpus_version,omitempty"`
	SchemaVersion string `json:"schema_version,omitempty"`
}

type ReplaySummary struct {
	ReplayID         string `json:"replay_id"`
	Package          string `json:"package"`
	CorpusName       string `json:"corpus_name"`
	CorpusVersion    int    `json:"corpus_version"`
	SchemaVersion    string `json:"schema_version"`
	Status           string `json:"status"`
	Total            int    `json:"total"`
	Succeeded        int    `json:"succeeded"`
	Failed           int    `json:"failed"`
	MissingFieldItems int  `json:"missing_field_items"`
	JSONDiffItems    int    `json:"json_diff_items"`
	StartedAt        string `json:"started_at"`
	UpdatedAt        string `json:"updated_at"`
	Error            string `json:"error,omitempty"`
}

type ReplayItemView struct {
	Key           string                `json:"key"`
	Name          string                `json:"name,omitempty"`
	Message       string                `json:"message"`
	Encoding      string                `json:"encoding"`
	Status        string                `json:"status"`
	Error         string                `json:"error,omitempty"`
	MissingFields []string              `json:"missing_fields,omitempty"`
	JSONDiffs     []replay.JSONDiff     `json:"json_diffs,omitempty"`
	DecodedJSON   string                `json:"decoded_json,omitempty"`
}

type ReplayResponse struct {
	Summary ReplaySummary    `json:"summary"`
	Items   []ReplayItemView `json:"items,omitempty"`
}
