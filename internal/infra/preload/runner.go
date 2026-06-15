package preload

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Sentinel errors returned by RunWithPreload to let callers
// distinguish which phase failed without parsing error strings.
// Errors from proposer.Propose are returned bare (no wrapping) so
// callers can attribute them to Raft-queue diagnostics.
var (
	ErrMarshalProposal      = errors.New("marshaling proposal")
	ErrAcquireProposalGuard = errors.New("acquiring proposal guard")
)

// Proposer is the canonical interface for submitting a built Raft
// proposal. Implemented by *node.Node when the caller acquires the
// IndexTracker mutex externally (which RunWithPreload does via the
// ProposalGuard) and by *node.LockedProposer for callers that bypass
// the guard.
type Proposer interface {
	Propose(ctx context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error)
}

// RunResult carries the outcome of RunWithPreload plus per-phase
// timing and rebuild signaling. Callers use the timings to drive
// their own metrics (admission and mirror each emit different
// histograms / counters for the same underlying phases).
type RunResult struct {
	// Proposal is the *node.Proposal that was submitted. Its embedded
	// future resolves when Raft accepts the proposal; call
	// Proposal.Wait() to block on Raft acceptance.
	Proposal *node.Proposal

	// FSMFuture resolves to the FSM apply result once the proposal
	// is applied. Call FSMFuture.Wait() after Proposal.Wait() to
	// block on FSM apply.
	FSMFuture *futures.Future[state.ApplyResult]

	// Guard exposes loader-cleanup. The proposal-lock side is already
	// released by RunWithPreload before returning; the caller must
	// invoke ReleaseLoaders at the appropriate time (immediately for
	// single-shot callers, deferred until FSM apply for admission's
	// loader dedup across concurrent admissions).
	Guard *ProposalGuard

	// Rebuilt is true when AcquireProposalGuard detected a next-index
	// generation shift between BuildPreloads and lock acquisition and
	// rebuilt the PreloadSet under the lock. Callers can surface this
	// as a counter to track contention.
	Rebuilt bool

	// LockHeldDuration is the time spent holding the proposal lock —
	// from AcquireProposalGuard start through proposer.Propose return.
	// This is what admission's "proposal guard duration" metric
	// measures: the contended portion of the path.
	LockHeldDuration time.Duration

	// ProposeDuration is the proposer.Propose call alone (Raft queue
	// insertion + initial replication, not FSM apply).
	ProposeDuration time.Duration

	// ProposeStartTime marks the wall-clock instant right before the
	// proposer.Propose call. Callers that want a "Propose + Wait"
	// metric (the time from queue insertion through Raft commit) record
	// time.Since(result.ProposeStartTime) after Proposal.Wait() returns.
	ProposeStartTime time.Time
}

// RunWithPreload is the single canonical "attach preload + propose"
// path shared by admission, mirror, and the metadata converter.
//
// The caller is responsible for:
//   - Calling BuildPreloads (and emitting whatever metrics it likes
//     around that call: hit/miss, total keys, duration).
//   - Marshaling the command via the marshalFn callback, which gets
//     invoked once on the happy path and once more on the rare
//     rebuild path. The callback is the right place to record
//     marshal duration / proposal size.
//   - Releasing the loaders via guard.ReleaseLoaders() at the
//     appropriate time.
//
// On the happy path the runner:
//  1. attaches build.PreloadSet to cmd,
//  2. invokes marshalFn to serialize cmd,
//  3. acquires the proposal guard (IndexTracker mutex),
//  4. patches cmd.PredictedIndex from the tracker's next index,
//  5. re-marshals via marshalFn if the next-index generation shifted,
//  6. submits the proposal via proposer.Propose,
//  7. releases the proposal-lock side of the guard.
//
// PredictedIndex is always set. It is a no-cost backstop: the FSM
// rejects with ErrStaleProposal if the actual Raft index doesn't
// match, catching cases where the tracker inflated due to a dropped
// proposal (e.g. leadership transition).
func (p *Preloader) RunWithPreload(
	ctx context.Context,
	cmd *raftcmdpb.Proposal,
	build *PreloadBuild,
	needs *Needs,
	marshalFn func(*raftcmdpb.Proposal) ([]byte, error),
	proposer Proposer,
) (*RunResult, error) {
	// Guarantee a unique command ID. The applier keys FSM futures by
	// `cmd.Id` (`node.NewProposal(cmd.GetId(), data)` below); two in-flight
	// proposals sharing Id=0 would overwrite each other in
	// `Applier.StoreFuture`, hanging one waiter and routing the apply
	// result to the wrong caller. Several technical callers build
	// `&raftcmdpb.Proposal{...}` directly (metadata conversion batches /
	// completions, IndexReady, cluster config, idempotency eviction)
	// without going through `commands.NewCommand`, so the runner backstops
	// here rather than requiring discipline at every call site.
	if cmd.GetId() == 0 {
		cmd.Id = commands.GenerateRandomID()
	}

	cmd.Preload = build.PreloadSet

	data, err := marshalFn(cmd)
	if err != nil {
		build.ReleaseLoaders()

		return nil, fmt.Errorf("%w: %w", ErrMarshalProposal, err)
	}

	guardStart := time.Now()

	updatedPreloads, guard, err := p.AcquireProposalGuard(build, needs)
	if err != nil {
		if guard != nil {
			guard.ReleaseAll()
		} else {
			// AcquireProposalGuard failed before building a guard, so
			// nobody owns the loaders we just acquired. Release them
			// directly to avoid pinning loader/cache references.
			build.ReleaseLoaders()
		}

		return nil, fmt.Errorf("%w: %w", ErrAcquireProposalGuard, err)
	}

	cmd.PredictedIndex = p.TrackerNext()

	result := &RunResult{Guard: guard}

	if updatedPreloads != nil {
		// Rare: next-index generation shifted between BuildPreloads
		// and AcquireProposalGuard. The PreloadSet was rebuilt under
		// the proposal lock; re-marshal the entire Proposal because
		// the rebuild changes preload boundaries and possibly values.
		result.Rebuilt = true
		cmd.Preload = updatedPreloads

		data, err = marshalFn(cmd)
		if err != nil {
			guard.ReleaseAll()

			return nil, fmt.Errorf("%w (under guard): %w", ErrMarshalProposal, err)
		}
	} else {
		// Common path: patch the PredictedIndex onto the pre-marshaled
		// buffer rather than re-marshaling the whole proposal (which
		// can be megabytes for large batches).
		data = AppendProposalPredictedIndex(data, cmd.GetPredictedIndex())
	}

	proposal := node.NewProposal(cmd.GetId(), data)

	result.ProposeStartTime = time.Now()
	fsmFuture, err := proposer.Propose(ctx, proposal)
	result.ProposeDuration = time.Since(result.ProposeStartTime)

	if err != nil {
		guard.ReleaseAll()

		return nil, err
	}

	guard.Release()
	result.LockHeldDuration = time.Since(guardStart)
	result.Proposal = proposal
	result.FSMFuture = fsmFuture

	return result, nil
}
