package registry

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/reflect/protoregistry"

	"protocompat/internal/corpus"
)

// leaseTTL bounds how long a claimed item can stay unfinished before a
// crashed worker's claim is considered dead and reclaimable by a restart.
const leaseTTL = 30 * time.Second

// ErrRunInterrupted means RunReplay stopped before every item was done
// (typically because ctx was canceled). The run stays in "running" with
// its completed items persisted, so a later call resumes it.
var ErrRunInterrupted = errors.New("replay interrupted before completion")

// schemaLoader returns the live descriptor closure for a registered schema
// version.
type schemaLoader func() (*protoregistry.Files, error)

// RunReplay drives one run to completion (or to ctx cancellation). It is
// safe to call repeatedly for the same replay id: the claim/complete state
// machine guarantees each item produces exactly one result.
//
// Decode never errors for payload-level problems — those live on the
// ItemResult — so one malformed wire sample or unknown JSON key cannot
// abort the remaining samples.
func RunReplay(ctx context.Context, store CorpusStore, run *Replay, load schemaLoader) error {
	leaseFloor := time.Now().Add(-leaseTTL)
	files, err := load()
	if err != nil {
		// Schema-level failure is infrastructure, not a per-sample
		// outcome: fail the run rather than marking every item failed.
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return ErrRunInterrupted
		}
		item, err := store.ClaimNextItem(ctx, run.ID, leaseFloor)
		if err != nil {
			return err
		}
		if item == nil {
			break // nothing pending and no expired leases
		}

		result := corpus.Decode(files, item.Digest, item.Sample)

		if err := store.CompleteItem(ctx, run.ID, item.Position, result); err != nil {
			if errors.Is(err, ErrItemAlreadyComplete) {
				// An earlier incarnation already produced the one result
				// allowed for this input key; never overwrite it.
				continue
			}
			return err
		}
	}

	items, err := store.ListReplayItems(ctx, run.ID)
	if err != nil {
		return err
	}
	results := make([]corpus.ItemResult, 0, len(items))
	for _, it := range items {
		if it.Status != ItemCompleted || it.Result == nil {
			// A lease is still open (e.g. another worker / this worker
			// canceled mid-item): leave the run resumable.
			return ErrRunInterrupted
		}
		results = append(results, *it.Result)
	}
	if err := store.CompleteReplay(ctx, run.ID, ReplayCompleted, corpus.Summarize(results)); err != nil {
		// Another worker racing to finish may have flipped running ->
		// completed first; that is success, not an error.
		if errors.Is(err, ErrNotFound) {
			if fresh, gerr := store.GetReplay(ctx, run.Package, run.ID); gerr == nil && fresh.Status == ReplayCompleted {
				return nil
			}
		}
		return err
	}
	return nil
}
