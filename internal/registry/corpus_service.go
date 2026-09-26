package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"protocompat/internal/corpus"
	"protocompat/internal/schema"
)

// CorpusService serves the versioned-corpus and batch-replay API.
type CorpusService struct {
	store   Store
	corpora CorpusStore
}

// NewCorpusService builds the service over a store that implements both
// the schema and corpus surfaces (PGStore and MemStore do).
func NewCorpusService(store FullStore) *CorpusService {
	return &CorpusService{store: store, corpora: store}
}

// --- corpus set lifecycle ---

// CreateCorpus uploads a new draft. With base_version it derives the draft
// from a sealed set and applies add/remove; without it the corpus starts
// empty. Re-uploading a sample with identical content never creates a
// second sample.
func (s *CorpusService) CreateCorpus(ctx context.Context, req *connect.Request[CreateCorpusRequest]) (*connect.Response[CorpusResponse], error) {
	r := req.Msg
	if r.Package == "" || r.Corpus == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package and corpus are required"))
	}
	if err := validateCorpusName(r.Corpus); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	added, seen, err := s.ingestSamples(ctx, r.Package, r.Samples)
	if err != nil {
		return nil, err
	}
	allAdd := mergeDigests(added, seen, r.Add)

	set, created, err := s.corpora.CreateDraft(ctx, r.Package, r.Corpus, r.BaseVersion, r.Note, allAdd, r.Remove)
	switch {
	case errors.Is(err, ErrDraftExists):
		return connect.NewResponse(&CorpusResponse{
			Corpus:             corpusMeta(set),
			AlreadyExisted:     true,
			AddedSampleDigests: added,
			AlreadySeen:        seen,
		}), nil
	case errors.Is(err, ErrNotFound):
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("base corpus version %s/%s@%d not found", r.Package, r.Corpus, r.BaseVersion))
	case errors.Is(err, ErrNoChange):
		// A pure re-upload over a sealed base with no changes is an
		// idempotent acknowledgement, not an error.
		base, gerr := s.corpora.GetCorpusSet(ctx, r.Package, r.Corpus, r.BaseVersion)
		if gerr != nil {
			return nil, connect.NewError(connect.CodeInternal, gerr)
		}
		return connect.NewResponse(&CorpusResponse{
			Corpus: corpusMeta(base), AlreadyExisted: true,
			AddedSampleDigests: added, AlreadySeen: seen,
		}), nil
	case err != nil:
		return nil, mapCorpusError(err)
	}
	_ = created
	return connect.NewResponse(&CorpusResponse{
		Corpus: corpusMeta(set), AddedSampleDigests: added, AlreadySeen: seen,
	}), nil
}

// UpdateCorpus mutates the open draft: more samples, digest-based
// add/remove, or a new note. Sealed versions reject every mutation.
func (s *CorpusService) UpdateCorpus(ctx context.Context, req *connect.Request[UpdateCorpusRequest]) (*connect.Response[CorpusResponse], error) {
	r := req.Msg
	if r.Package == "" || r.Corpus == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package and corpus are required"))
	}
	added, seen, err := s.ingestSamples(ctx, r.Package, r.Samples)
	if err != nil {
		return nil, err
	}
	allAdd := mergeDigests(added, seen, r.Add)

	set, err := s.corpora.UpdateDraft(ctx, r.Package, r.Corpus, r.Note, allAdd, r.Remove)
	if err != nil {
		return nil, mapCorpusError(err)
	}
	return connect.NewResponse(&CorpusResponse{
		Corpus: corpusMeta(set), AddedSampleDigests: added, AlreadySeen: seen,
	}), nil
}

// SealCorpus freezes the draft. From this point the version is immutable:
// updates and deletes are refused.
func (s *CorpusService) SealCorpus(ctx context.Context, req *connect.Request[SealCorpusRequest]) (*connect.Response[CorpusResponse], error) {
	r := req.Msg
	if r.Package == "" || r.Corpus == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package and corpus are required"))
	}
	set, err := s.corpora.SealCorpus(ctx, r.Package, r.Corpus)
	if err != nil {
		return nil, mapCorpusError(err)
	}
	return connect.NewResponse(&CorpusResponse{Corpus: corpusMeta(set)}), nil
}

func (s *CorpusService) GetCorpus(ctx context.Context, req *connect.Request[GetCorpusRequest]) (*connect.Response[CorpusResponse], error) {
	r := req.Msg
	if r.Package == "" || r.Corpus == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package and corpus are required"))
	}
	var (
		set *CorpusSet
		err error
	)
	if r.Version == 0 {
		set, err = s.corpora.LatestCorpusVersion(ctx, r.Package, r.Corpus)
	} else {
		set, err = s.corpora.GetCorpusSet(ctx, r.Package, r.Corpus, r.Version)
	}
	if err != nil {
		return nil, mapCorpusError(err)
	}
	if set.Status == CorpusDeleted {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("corpus version was a draft that has been deleted"))
	}
	return connect.NewResponse(&CorpusResponse{Corpus: corpusMeta(set)}), nil
}

func (s *CorpusService) ListCorpora(ctx context.Context, req *connect.Request[ListCorporaRequest]) (*connect.Response[ListCorporaResponse], error) {
	if req.Msg.Package == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package is required"))
	}
	sets, err := s.corpora.ListCorpora(ctx, req.Msg.Package)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &ListCorporaResponse{Package: req.Msg.Package}
	for i := range sets {
		resp.Corpora = append(resp.Corpora, corpusMeta(&sets[i]))
	}
	return connect.NewResponse(resp), nil
}

// DeleteCorpusDraft removes the open draft only. Sealed history and every
// replay that references it are untouched.
func (s *CorpusService) DeleteCorpusDraft(ctx context.Context, req *connect.Request[DeleteCorpusDraftRequest]) (*connect.Response[DeleteCorpusDraftResponse], error) {
	r := req.Msg
	if r.Package == "" || r.Corpus == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package and corpus are required"))
	}
	if err := s.corpora.DeleteDraft(ctx, r.Package, r.Corpus); err != nil {
		return nil, mapCorpusError(err)
	}
	return connect.NewResponse(&DeleteCorpusDraftResponse{Deleted: true}), nil
}

// --- replays ---

// StartReplay replays a sealed corpus set against one registered schema
// version. The replay key makes the request idempotent: repeating it with
// the same key resumes the existing run instead of creating a second one,
// and never redoes completed items.
func (s *CorpusService) StartReplay(ctx context.Context, req *connect.Request[StartReplayRequest]) (*connect.Response[StartReplayResponse], error) {
	r := req.Msg
	if r.Package == "" || r.Corpus == "" || r.CorpusVersion == 0 || r.SchemaVersion == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package, corpus, corpus_version and schema_version are required"))
	}
	set, err := s.corpora.GetCorpusSet(ctx, r.Package, r.Corpus, r.CorpusVersion)
	if err != nil {
		return nil, mapCorpusError(err)
	}
	if set.Status != CorpusSealed {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("corpus %s@%d is %s; only sealed versions can be replayed", r.Corpus, r.CorpusVersion, set.Status))
	}
	schemaVer, err := s.store.GetVersion(ctx, r.Package, r.SchemaVersion)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("schema version %s/%s not found", r.Package, r.SchemaVersion))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	key := r.ReplayKey
	if key == "" {
		key = deterministicReplayKey(r.Package, r.Corpus, r.CorpusVersion, r.SchemaVersion)
	}

	items, err := s.snapshotItems(ctx, r.Package, set)
	if err != nil {
		return nil, err
	}

	run, err := s.corpora.CreateReplay(ctx, Replay{
		Package: r.Package, Key: key, Corpus: r.Corpus,
		CorpusVersion: set.Version, SetID: set.ID, SchemaVersion: r.SchemaVersion,
	}, items)
	alreadyExisted := false
	var existence *ReplayExistenceError
	if errors.As(err, &existence) {
		run = existence.Replay
		alreadyExisted = true
		err = nil
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	loader := func() (*protoregistry.Files, error) {
		return schema.Load(schemaVer.DescriptorSet)
	}
	// An already-finished run returned by its key is not driven again:
	// its results are the one record for that input key. A "failed" run
	// (previous infrastructure error) is resumable and can complete.
	if run.Status == ReplayRunning || run.Status == ReplayFailed {
		runErr := RunReplay(ctx, s.corpora, run, loader)
		if runErr != nil && !errors.Is(runErr, ErrRunInterrupted) {
			_ = s.corpora.CompleteReplay(ctx, run.ID, ReplayFailed, corpus.Summary{})
			return nil, connect.NewError(connect.CodeInternal, runErr)
		}
	}

	// Refresh after the run.
	fresh, gerr := s.corpora.GetReplay(ctx, r.Package, run.ID)
	if gerr != nil {
		return nil, connect.NewError(connect.CodeInternal, gerr)
	}
	resp := &StartReplayResponse{Replay: replayMeta(fresh), AlreadyExisted: alreadyExisted}
	if fresh.Status == ReplayCompleted {
		all, _ := s.corpora.ListReplayItems(ctx, fresh.ID)
		resp.Items = itemResults(all)
	}
	return connect.NewResponse(resp), nil
}

func (s *CorpusService) GetReplay(ctx context.Context, req *connect.Request[GetReplayRequest]) (*connect.Response[GetReplayResponse], error) {
	r := req.Msg
	if r.Package == "" || (r.ReplayID == 0 && r.ReplayKey == "") {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package and replay_id or replay_key are required"))
	}
	var (
		run *Replay
		err error
	)
	if r.ReplayKey != "" {
		run, err = s.corpora.GetReplayByKey(ctx, r.Package, r.ReplayKey)
	} else {
		run, err = s.corpora.GetReplay(ctx, r.Package, r.ReplayID)
	}
	if err != nil {
		return nil, mapCorpusError(err)
	}
	all, err := s.corpora.ListReplayItems(ctx, run.ID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &GetReplayResponse{Replay: replayMeta(run)}
	if run.Status == ReplayCompleted {
		resp.Items = itemResults(all)
	}
	return connect.NewResponse(resp), nil
}

func (s *CorpusService) ListReplays(ctx context.Context, req *connect.Request[ListReplaysRequest]) (*connect.Response[ListReplaysResponse], error) {
	if req.Msg.Package == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package is required"))
	}
	runs, err := s.corpora.ListReplays(ctx, req.Msg.Package, req.Msg.Corpus)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &ListReplaysResponse{Package: req.Msg.Package}
	for i := range runs {
		resp.Replays = append(resp.Replays, replayMeta(&runs[i]))
	}
	return connect.NewResponse(resp), nil
}

// CompareReplays returns the stable difference between two completed runs
// of the same corpus (typically old vs new schema versions), aligned by
// sample digest.
func (s *CorpusService) CompareReplays(ctx context.Context, req *connect.Request[CompareReplaysRequest]) (*connect.Response[CompareReplaysResponse], error) {
	r := req.Msg
	if r.Package == "" || r.OldReplayID == 0 || r.NewReplayID == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package, old_replay_id and new_replay_id are required"))
	}
	oldRun, err := s.corpora.GetReplay(ctx, r.Package, r.OldReplayID)
	if err != nil {
		return nil, mapCorpusError(err)
	}
	newRun, err := s.corpora.GetReplay(ctx, r.Package, r.NewReplayID)
	if err != nil {
		return nil, mapCorpusError(err)
	}
	if oldRun.Status != ReplayCompleted || newRun.Status != ReplayCompleted {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("both replays must be completed before they can be compared"))
	}
	oldItems, err := s.corpora.ListReplayItems(ctx, oldRun.ID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	newItems, err := s.corpora.ListReplayItems(ctx, newRun.ID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	cmp := corpus.CompareReplays(
		oldRun.SchemaVersion, newRun.SchemaVersion,
		fmt.Sprintf("%d", oldRun.ID), fmt.Sprintf("%d", newRun.ID),
		itemResults(oldItems), itemResults(newItems))
	return connect.NewResponse(&CompareReplaysResponse{Comparison: cmp}), nil
}

// --- internals ---

// ingestSamples validates and content-addresses inline samples. It returns
// digests newly created, digests of identical already-known content, and
// the union digest list to add.
func (s *CorpusService) ingestSamples(ctx context.Context, pkg string, uploads []CorpusUploadSample) (added, seen []string, err error) {
	for _, u := range uploads {
		sample := corpus.Sample{
			Name: u.Name, Message: u.Message, Encoding: u.Encoding,
			Data: u.Data, Expectation: u.Expectation,
		}
		if verr := corpus.ValidateUpload(sample); verr != nil {
			return nil, nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("sample %q: %w", sampleLabel(sample), verr))
		}
		digest, derr := corpus.DigestOf(sample)
		if derr != nil {
			return nil, nil, connect.NewError(connect.CodeInvalidArgument, derr)
		}
		created, perr := s.corpora.PutSample(ctx, pkg, CorpusSampleRecord{
			Digest: digest, Name: sample.Name, Message: sample.Message,
			Encoding: sample.Encoding, Data: sample.Data, Expectation: sample.Expectation,
		})
		if perr != nil {
			return nil, nil, connect.NewError(connect.CodeInternal, perr)
		}
		if created {
			added = append(added, digest)
		} else {
			seen = append(seen, digest)
		}
	}
	return added, seen, nil
}

func (s *CorpusService) snapshotItems(ctx context.Context, pkg string, set *CorpusSet) ([]ReplayItem, error) {
	items := make([]ReplayItem, 0, len(set.Digests))
	for _, digest := range set.Digests {
		rec, err := s.corpora.GetSample(ctx, pkg, digest)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal,
				fmt.Errorf("snapshot sample %s: %w", digest, err))
		}
		items = append(items, ReplayItem{
			Digest: digest,
			Sample: corpus.Sample{
				Name: rec.Name, Message: rec.Message, Encoding: rec.Encoding,
				Data: rec.Data, Expectation: rec.Expectation,
			},
		})
	}
	return items, nil
}

func mergeDigests(added, seen, extra []string) []string {
	set := map[string]bool{}
	var out []string
	for _, d := range append(append(append([]string{}, added...), seen...), extra...) {
		if d == "" || set[d] {
			continue
		}
		set[d] = true
		out = append(out, d)
	}
	return out
}

func deterministicReplayKey(pkg, corpusName string, corpusVersion int, schemaVersion string) string {
	h := sha256.Sum256([]byte(strings.Join([]string{
		pkg, corpusName, fmt.Sprintf("%d", corpusVersion), schemaVersion,
	}, "\x00")))
	return "auto-" + hex.EncodeToString(h[:16])
}

func validateCorpusName(name string) error {
	if len(name) > 200 || strings.ContainsAny(name, "/\n\r\x00") {
		return fmt.Errorf("invalid corpus name %q", name)
	}
	return nil
}

func sampleLabel(s corpus.Sample) string {
	if s.Name != "" {
		return s.Name
	}
	return s.Message
}

func corpusMeta(set *CorpusSet) CorpusMeta {
	m := CorpusMeta{
		Package: set.Package, Corpus: set.Corpus, Version: set.Version,
		Status: set.Status, Note: set.Note, SampleCount: set.SampleCount,
		Digests: set.Digests, CreatedAt: formatTime(set.CreatedAt),
	}
	if set.SealedAt != nil {
		m.SealedAt = formatTime(*set.SealedAt)
	}
	return m
}

func replayMeta(r *Replay) ReplayMeta {
	return ReplayMeta{
		ID: r.ID, Package: r.Package, ReplayKey: r.Key, Corpus: r.Corpus,
		CorpusVersion: r.CorpusVersion, SchemaVersion: r.SchemaVersion,
		Status: r.Status, Total: r.Total, Completed: r.Completed,
		Summary: r.Summary, CreatedAt: formatTime(r.CreatedAt), UpdatedAt: formatTime(r.UpdatedAt),
	}
}

func itemResults(items []ReplayItem) []corpus.ItemResult {
	sort.Slice(items, func(i, j int) bool { return items[i].Position < items[j].Position })
	out := make([]corpus.ItemResult, 0, len(items))
	for _, it := range items {
		if it.Result != nil {
			out = append(out, *it.Result)
		}
	}
	return out
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func mapCorpusError(err error) *connect.Error {
	switch {
	case errors.Is(err, ErrNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, ErrSealed):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, ErrNoDraft):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, ErrNoChange):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}
