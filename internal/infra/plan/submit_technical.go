package plan

import (
	"context"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// maxTechnicalStaleRetries bounds the number of times SubmitTechnical
// retries an ErrStaleProposal rejection before giving up. Stale rejections
// happen when the IndexTracker is inflated from a dropped proposal (e.g.
// leadership transition); a fresh PredictedIndex is computed on every
// re-attempt, so once the tracker catches up the next try succeeds.
const maxTechnicalStaleRetries = 5

// SubmitTechnical submits a technical Raft proposal through the preload
// runner, owning the retry/cleanup/wait lifecycle that bootstrap
// technical proposals and event-sink cursor updates used to duplicate.
//
// The caller supplies:
//   - cmd, already stamped with its TechnicalUpdates, Date and
//     CallerSnapshot. SubmitTechnical only resets the per-attempt fields
//     (Id, PredictedIndex, ExecutionPlan) so Run assigns fresh values on
//     every attempt.
//   - operations, declaring each TechnicalUpdate's read Coverage (nil or
//     empty Coverage for handlers that read nothing — cluster config,
//     idempotency eviction, events sink, backup).
//   - identity, the caller name used in error messages.
//
// Why route technical proposals through the preload runner:
//   - PredictedIndex is set as a backstop: a stale tracker (e.g. a
//     dropped proposal during leadership transition) causes the FSM
//     to reject with ErrStaleProposal rather than silently apply
//     against an inconsistent state.
//   - All proposals go through one canonical path, so the IndexTracker
//     lock and Raft propose are serialized identically everywhere.
//
// SubmitTechnical blocks until the FSM applies and retries
// ErrStaleProposal up to maxTechnicalStaleRetries times before giving up.
// Non-stale errors (including domain errors other than ErrStaleProposal)
// propagate unwrapped so callers can attribute them via errors.Is.
func SubmitTechnical(
	ctx context.Context,
	builder *Builder,
	proposer Proposer,
	cmd *raftcmdpb.Proposal,
	operations []WriteOperation,
	identity string,
) error {
	var lastErr error

	for range maxTechnicalStaleRetries {
		err := submitTechnicalOnce(ctx, builder, proposer, cmd, operations, identity)
		if err == nil {
			return nil
		}

		if !errors.Is(err, domain.ErrStaleProposal) {
			return err
		}

		lastErr = err
	}

	return fmt.Errorf("%s: giving up after %d stale retries: %w", identity, maxTechnicalStaleRetries, lastErr)
}

func submitTechnicalOnce(ctx context.Context, builder *Builder, proposer Proposer, cmd *raftcmdpb.Proposal, operations []WriteOperation, identity string) error {
	// Reset the per-attempt fields so Run assigns a fresh ID and
	// PredictedIndex on each retry (the previous stale rejection left
	// them populated). The caller-supplied technical updates, caller
	// snapshot and date are left intact.
	cmd.Id = 0
	cmd.PredictedIndex = 0
	cmd.ExecutionPlan = nil

	// Technical proposals are cold-path (cluster config, idempotency
	// eviction, sink cursor, backup) — a rare per-call aggregate Merge is
	// cheaper than plumbing the aggregate through every caller.
	aggregate := NewCoverage()
	for _, op := range operations {
		aggregate.Merge(op.Coverage)
	}

	build, err := builder.Build(aggregate, operations)
	if err != nil {
		if build != nil {
			build.ReleaseLoaders()
		}

		return fmt.Errorf("building preloads for %s: %w", identity, err)
	}

	result, err := builder.Run(
		ctx, cmd, build,
		func(c *raftcmdpb.Proposal) ([]byte, error) { return c.MarshalVT() },
		proposer,
	)
	if err != nil {
		return err
	}

	result.Guard.ReleaseLoaders()

	return waitTechnical(ctx, result, identity)
}

// waitTechnical blocks on Raft acceptance then FSM apply, returning the
// first error encountered. The caller's ctx (typically derived from a
// stop channel, or the emitter's bounded cursor-update timeout) cancels
// the wait when the node stops or loses leadership after Raft acceptance
// but before FSM apply, letting the caller observe the shutdown instead
// of hanging forever.
func waitTechnical(ctx context.Context, result *RunResult, identity string) error {
	if _, err := result.Proposal.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for raft acceptance: %w", err)
	}

	res, err := result.FSMFuture.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for FSM apply: %w", err)
	}

	if res.Error != nil {
		// Wrap with %w so callers (and SubmitTechnical's retry loop)
		// can detect ErrStaleProposal via errors.Is.
		return fmt.Errorf("applying %s: %w", identity, res.Error)
	}

	return nil
}
