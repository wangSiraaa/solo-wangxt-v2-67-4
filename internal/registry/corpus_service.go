package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"

	"protocompat/internal/replay"
	"protocompat/internal/schema"
)

// CreateCorpusSet creates a draft. Omit version to append after the latest
// version; base_version defaults to the latest sealed version (-1 disables
// inheritance).
func (s *Service) CreateCorpusSet(ctx context.Context, req *connect.Request[CreateCorpusSetRequest]) (*connect.Response[CorpusSetResponse], error) {
	r := req.Msg
	if r.Package == "" || r.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package and corpus name are required"))
	}
	latest, err := s.store.LatestCorpusSet(ctx, r.Package, r.Name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	version := r.Version
	if version == 0 {
		if latest != nil {
			version = latest.Version + 1
		} else {
			version = 1
		}
	}
	if version <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("corpus version must be positive"))
	}
	var base *int
	if latest != nil && latest.Status != CorpusStatusSealed {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("latest corpus version %d is a draft; seal or delete it before creating another", latest.Version))
	}
	switch {
	case r.BaseVersion < 0:
		base = nil
	case r.BaseVersion > 0:
		b := r.BaseVersion
		baseSet, err := s.store.GetCorpusSet(ctx, r.Package, r.Name, b)
		if err != nil {
			return nil, corpusErr(err)
		}
		if baseSet.Status != CorpusStatusSealed {
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("base corpus version %d is not sealed", b))
		}
		base = &b
	case latest != nil:
		b := latest.Version
		base = &b
	}
	err = s.store.CreateCorpusDraft(ctx, r.Package, r.Name, version, base, CorpusMutation{AddOrUpdate: r.AddOrUpdate, DeleteKeys: r.DeleteKeys})
	if err != nil {
		return nil, corpusErr(err)
	}
	return s.corpusResponse(ctx, r.Package, r.Name, version)
}

// UpdateCorpusSet applies an add/update/delete delta to a draft. Re-uploading
// the same content is an idempotent no-op; sealed versions are rejected.
func (s *Service) UpdateCorpusSet(ctx context.Context, req *connect.Request[UpdateCorpusSetRequest]) (*connect.Response[CorpusSetResponse], error) {
	r := req.Msg
	if r.Package == "" || r.Name == "" || r.Version <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package, corpus name and positive version are required"))
	}
	if err := s.store.MutateCorpusDraft(ctx, r.Package, r.Name, r.Version, CorpusMutation{AddOrUpdate: r.AddOrUpdate, DeleteKeys: r.DeleteKeys}); err != nil {
		return nil, corpusErr(err)
	}
	return s.corpusResponse(ctx, r.Package, r.Name, r.Version)
}

func (s *Service) SealCorpusSet(ctx context.Context, req *connect.Request[SealCorpusSetRequest]) (*connect.Response[CorpusSetResponse], error) {
	r := req.Msg
	if r.Package == "" || r.Name == "" || r.Version <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package, corpus name and positive version are required"))
	}
	if err := s.store.SealCorpusDraft(ctx, r.Package, r.Name, r.Version); err != nil {
		return nil, corpusErr(err)
	}
	return s.corpusResponse(ctx, r.Package, r.Name, r.Version)
}

func (s *Service) DeleteCorpusDraft(ctx context.Context, req *connect.Request[DeleteCorpusDraftRequest]) (*connect.Response[DeleteCorpusDraftResponse], error) {
	r := req.Msg
	if r.Package == "" || r.Name == "" || r.Version <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package, corpus name and positive version are required"))
	}
	if err := s.store.DeleteCorpusDraft(ctx, r.Package, r.Name, r.Version); err != nil {
		return nil, corpusErr(err)
	}
	return connect.NewResponse(&DeleteCorpusDraftResponse{Deleted: true}), nil
}

func (s *Service) GetCorpusSet(ctx context.Context, req *connect.Request[GetCorpusSetRequest]) (*connect.Response[CorpusSetResponse], error) {
	r := req.Msg
	if r.Package == "" || r.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package and corpus name are required"))
	}
	return s.corpusResponse(ctx, r.Package, r.Name, r.Version)
}

func (s *Service) ListCorpusSets(ctx context.Context, req *connect.Request[ListCorpusSetsRequest]) (*connect.Response[ListCorpusSetsResponse], error) {
	if req.Msg.Package == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package is required"))
	}
	sets, err := s.store.ListCorpusSets(ctx, req.Msg.Package, req.Msg.Name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &ListCorpusSetsResponse{Package: req.Msg.Package}
	for i := range sets {
		meta, err := s.corpusMeta(ctx, &sets[i])
		if err != nil {
			return nil, err
		}
		resp.Sets = append(resp.Sets, meta)
	}
	return connect.NewResponse(resp), nil
}

// StartReplay creates or idempotently returns the single replay for the
// (package, corpus, corpus version, schema version) tuple.
func (s *Service) StartReplay(ctx context.Context, req *connect.Request[StartReplayRequest]) (*connect.Response[ReplayResponse], error) {
	r := req.Msg
	if r.Package == "" || r.CorpusName == "" || r.SchemaVersion == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("package, corpus_name and schema_version are required"))
	}
	corpusSet, err := s.resolveCorpusSet(ctx, r.Package, r.CorpusName, r.CorpusVersion)
	if err != nil {
		return nil, err
	}
	if _, err := s.store.GetVersion(ctx, r.Package, r.SchemaVersion); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("schema version %s/%s not found", r.Package, r.SchemaVersion))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	started := Replay{
		ID:            replayID(r.Package, r.CorpusName, corpusSet.Version, r.SchemaVersion),
		Package:       r.Package,
		CorpusName:    r.CorpusName,
		CorpusVersion: corpusSet.Version,
		SchemaVersion: r.SchemaVersion,
	}
	stored, created, err := s.store.StartReplay(ctx, started)
	if errors.Is(err, ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("sealed corpus or schema version not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	_ = created
	return s.replayResponse(ctx, stored.ID, false)
}

func (s *Service) GetReplay(ctx context.Context, req *connect.Request[GetReplayRequest]) (*connect.Response[ReplayResponse], error) {
	r := req.Msg
	id := r.ReplayID
	if id == "" {
		if r.Package == "" || r.CorpusName == "" || r.SchemaVersion == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("replay_id or package/corpus/schema coordinates are required"))
		}
		corpusVersion := r.CorpusVersion
		if corpusVersion == 0 {
			set, cerr := s.store.LatestCorpusSet(ctx, r.Package, r.CorpusName)
			if cerr != nil {
				return nil, corpusErr(cerr)
			}
			corpusVersion = set.Version
		}
		stored, err := s.store.GetReplayByTarget(ctx, r.Package, r.CorpusName, r.SchemaVersion, corpusVersion)
		if err != nil {
			return nil, corpusErr(err)
		}
		id = stored.ID
	}
	return s.replayResponse(ctx, id, true)
}

// ProcessPendingReplays advances resumable replay work. It is driven by the
// server ticker and directly by tests/CLI synchronous waits. Each item is
// claimed before execution; already terminal results are never overwritten.
func (s *Service) ProcessPendingReplays(ctx context.Context, maxItems int) (int, error) {
	now := time.Now()
	pending, err := s.store.ListPendingReplays(ctx, now)
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, rep := range pending {
		if maxItems > 0 && processed >= maxItems {
			break
		}
		version, err := s.store.GetVersion(ctx, rep.Package, rep.SchemaVersion)
		if err != nil {
			return processed, err
		}
		compiled, err := schema.Load(version.DescriptorSet)
		if err != nil {
			return processed, err
		}
		for maxItems == 0 || processed < maxItems {
			now = time.Now()
			claimed, err := s.store.ClaimReplayItem(ctx, rep.ID, now, now.Add(2*time.Minute))
			if errors.Is(err, ErrNotFound) {
				break
			}
			if err != nil {
				return processed, err
			}
			sample, err := s.store.GetCorpusSample(ctx, rep.Package, claimed.Item.SampleID)
			if err != nil {
				_ = s.store.FinishReplayItem(ctx, rep.ID, claimed.Item.SampleID, replay.Result{Status: replay.StatusFailed, Error: err.Error()}, time.Now())
				processed++
				continue
			}
			result := replay.Decode(compiled, replay.Sample{
				Message: sample.Message, Encoding: sample.Encoding, Data: sample.Data, Expected: sample.ExpectedResult,
			})
			if err := s.store.FinishReplayItem(ctx, rep.ID, claimed.Item.SampleID, result, time.Now()); err != nil {
				return processed, err
			}
			processed++
		}
	}
	return processed, nil
}

// RecoverInterruptedReplays clears leases left by a previous process. This
// deployment runs one worker; the short lease is still the runtime safety net.
func (s *Service) RecoverInterruptedReplays(ctx context.Context) error {
	replays, err := s.store.ListPendingReplays(ctx, time.Now().Add(time.Hour))
	if err != nil {
		return err
	}
	now := time.Now()
	for _, rep := range replays {
		if rep.Status == ReplayStatusRunning {
			if err := s.store.ReopenReplay(ctx, rep.ID, now); err != nil {
				return err
			}
		}
	}
	return nil
}

// StartBackgroundRunner resumes unfinished replays after restart and keeps
// processing work until ctx is canceled.
func (s *Service) StartBackgroundRunner(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = s.ProcessPendingReplays(ctx, 0)
			}
		}
	}()
}

func (s *Service) resolveCorpusSet(ctx context.Context, pkg, name string, version int) (*CorpusSet, *connect.Error) {
	set, err := s.store.GetCorpusSet(ctx, pkg, name, version)
	if err != nil {
		return nil, corpusErr(err)
	}
	if set.Status != CorpusStatusSealed {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("corpus %s/%s version %d must be sealed before replay", pkg, name, set.Version))
	}
	return set, nil
}

func (s *Service) corpusResponse(ctx context.Context, pkg, name string, version int) (*connect.Response[CorpusSetResponse], error) {
	set, err := s.store.GetCorpusSet(ctx, pkg, name, version)
	if err != nil {
		return nil, corpusErr(err)
	}
	meta, err := s.corpusMeta(ctx, set)
	if err != nil {
		return nil, err
	}
	samples, err := s.store.ListCorpusSamples(ctx, set.ID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &CorpusSetResponse{Meta: meta}
	for _, env := range samples {
		resp.Samples = append(resp.Samples, corpusSampleView(env))
	}
	return connect.NewResponse(resp), nil
}

func (s *Service) corpusMeta(ctx context.Context, set *CorpusSet) (CorpusSetMeta, *connect.Error) {
	samples, err := s.store.ListCorpusSamples(ctx, set.ID)
	if err != nil {
		return CorpusSetMeta{}, connect.NewError(connect.CodeInternal, err)
	}
	meta := CorpusSetMeta{
		Name: set.Name, Version: set.Version, Status: set.Status, Base: set.Base,
		Total: len(samples), CreatedAt: formatTime(set.CreatedAt),
	}
	if set.SealedAt != nil {
		meta.SealedAt = formatTime(*set.SealedAt)
	}
	return meta, nil
}

func (s *Service) replayResponse(ctx context.Context, id string, includeItems bool) (*connect.Response[ReplayResponse], error) {
	rep, err := s.store.GetReplay(ctx, id)
	if err != nil {
		return nil, corpusErr(err)
	}
	resp := &ReplayResponse{Summary: replaySummary(rep)}
	if !includeItems {
		return connect.NewResponse(resp), nil
	}
	results, err := s.store.ListReplayResults(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	set, err := s.store.GetCorpusSet(ctx, rep.Package, rep.CorpusName, rep.CorpusVersion)
	if err != nil {
		return nil, corpusErr(err)
	}
	envelopes, err := s.store.ListCorpusSamples(ctx, set.ID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	byID := map[int64]CorpusSampleEnvelope{}
	for _, env := range envelopes {
		byID[env.Sample.ID] = env
	}
	for _, item := range results {
		env := byID[item.SampleID]
		view := ReplayItemView{
			Key: item.Key, Name: env.Membership.Name, Message: env.Sample.Message, Encoding: env.Sample.Encoding,
			Status: item.Status, Error: item.Error, MissingFields: item.MissingFields, JSONDiffs: item.JSONDiffs,
			DecodedJSON: item.DecodedJSON,
		}
		resp.Items = append(resp.Items, view)
	}
	return connect.NewResponse(resp), nil
}

func corpusSampleView(env CorpusSampleEnvelope) CorpusSampleView {
	return CorpusSampleView{
		Key: env.Membership.Key, Name: env.Membership.Name, Message: env.Sample.Message, Encoding: env.Sample.Encoding,
		Data: env.Sample.Data, ExpectedResult: env.Sample.ExpectedResult, ContentSummary: env.Sample.ContentSummary,
		Fingerprint: hex.EncodeToString(env.Sample.Fingerprint), CreatedAt: formatTime(env.Sample.CreatedAt),
	}
}

func replaySummary(r *Replay) ReplaySummary {
	return ReplaySummary{
		ReplayID: r.ID, Package: r.Package, CorpusName: r.CorpusName, CorpusVersion: r.CorpusVersion,
		SchemaVersion: r.SchemaVersion, Status: r.Status, Total: r.Total, Succeeded: r.Succeeded,
		Failed: r.Failed, MissingFieldItems: r.MissingFieldItems, JSONDiffItems: r.JSONDiffItems,
		StartedAt: formatTime(r.StartedAt), UpdatedAt: formatTime(r.UpdatedAt), Error: r.LastUnclaimedError,
	}
}

func corpusErr(err error) *connect.Error {
	switch {
	case errors.Is(err, ErrSealed):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, ErrCorpusConflict):
		return connect.NewError(connect.CodeAlreadyExists, err)
	case errors.Is(err, ErrNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

func replayID(pkg, corpus string, corpusVersion int, schemaVersion string) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%s", pkg, corpus, corpusVersion, schemaVersion)))
	return hex.EncodeToString(h[:16])
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05.000000Z")
}
