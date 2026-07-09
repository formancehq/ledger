package plan

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

// Sentinel errors returned by Run to let callers distinguish which
// phase failed without parsing error strings. Errors from
// proposer.Propose are returned bare (no wrapping) so callers can
// attribute them to Raft-queue diagnostics.
var (
	ErrMarshalProposal      = errors.New("marshaling proposal")
	ErrAcquireProposalGuard = errors.New("acquiring proposal guard")
)

// Proposer is the canonical interface for submitting a built Raft
// proposal. Implemented by *node.Node. Run holds the IndexTracker
// mutex itself around the Propose call (both on the slow path via
// ProposalGuard and on the no-preload fast path), so wrapper
// implementations must not re-acquire that lock.
type Proposer interface {
	Propose(ctx context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error)
}

// RunResult carries the outcome of Run plus per-phase timing and
// rebuild signaling. Callers use the timings to drive their own
// metrics (admission and mirror each emit different histograms /
// counters for the same underlying phases).
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
	// released by Run before returning; the caller must invoke
	// ReleaseLoaders at the appropriate time (immediately for
	// single-shot callers, deferred until FSM apply for admission's
	// loader dedup across concurrent admissions).
	Guard *ProposalGuard

	// Rebuilt is true when AcquireProposalGuard detected a next-index
	// generation shift between Build and lock acquisition and
	// rebuilt the ExecutionPlan under the lock. Callers can surface this
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

// Run is the single canonical "attach preload + assign bits + propose"
// path shared by admission, mirror, the metadata converter, the events
// emitter, and every other proposer.
//
// The caller is responsible for:
//   - Calling Build with the slice of WriteOperations the proposal
//     carries (emitting whatever metrics it likes around that call).
//   - Marshaling the command via the marshalFn callback (proto marshal
//     plus caller-specific instrumentation like commandSize.Record).
//   - Releasing the loaders via guard.ReleaseLoaders() at the
//     appropriate time.
//
// Run assigns coverage_bits (and production_bits when a productions
// assigner was attached via WithProductions) on each operation right
// before every marshalFn invocation — both on the happy path and on the
// rare rebuild path under the proposal guard. The caller's marshalFn
// must not touch those fields.
//
// On the happy path Run:
//  1. attaches build.ExecutionPlan to cmd,
//  2. iterates build.operations to assign coverage bits over plans,
//  3. invokes the productions assigner if any,
//  4. calls marshalFn to serialize cmd,
//  5. acquires the proposal guard (IndexTracker mutex),
//  6. patches cmd.PredictedIndex from the tracker's next index,
//  7. re-runs steps 2-4 + marshal if the next-index generation shifted,
//  8. submits the proposal via proposer.Propose,
//  9. releases the proposal-lock side of the guard.
//
// PredictedIndex is always set. It is a no-cost backstop: the FSM
// rejects with ErrStaleProposal if the actual Raft index doesn't
// match, catching cases where the tracker inflated due to a dropped
// proposal (e.g. leadership transition).
func (p *Builder) Run(
	ctx context.Context,
	cmd *raftcmdpb.Proposal,
	build *BuildResult,
	marshalFn func(*raftcmdpb.Proposal) ([]byte, error),
	proposer Proposer,
) (*RunResult, error) {
	// Guarantee a unique command ID. The applier keys FSM futures by
	// `cmd.Id` (`node.NewProposal(cmd.GetId(), data)` below); two in-flight
	// proposals sharing Id=0 would overwrite each other in
	// `Applier.StoreFuture`, hanging one waiter and routing the apply
	// result to the wrong caller. Several technical callers build
	// `&raftcmdpb.Proposal{...}` directly without going through
	// `commands.NewCommand`, so the runner backstops here rather than
	// requiring discipline at every call site.
	if cmd.GetId() == 0 {
		cmd.Id = commands.GenerateRandomID()
	}

	// Fast path: when the aggregated cache-attribute needs are empty
	// (technical updates whose handlers only write — EventsSink,
	// IdempotencyEviction, ClusterConfig — as well as orders that
	// only touch the IdempotencyStore such as maintenance, signing,
	// chapter-schedule operations), the generation revalidation under
	// AcquireProposalGuard has nothing to revalidate. Skip the guard
	// and only hold the tracker mutex long enough to inject
	// PredictedIndex and push into Raft's queue.
	//
	// Gating on AttributeKeysCount (not TotalKeys) so an idempotent
	// no-read order — whose Coverage carry IdempotencyKeys but no cache
	// attribute — also takes the fast path. Otherwise the slow path
	// would serialize a cache_epoch on cmd, and a cluster-config
	// cache reset between Build and apply would spuriously reject the
	// proposal as stale even though it never consulted the cache.
	if build.aggregate.AttributeKeysCount() == 0 {
		return p.runWithoutPreload(ctx, cmd, build, marshalFn, proposer)
	}

	cmd.ExecutionPlan = build.ExecutionPlan
	build.applyBits(cmd, build.ExecutionPlan.GetAttributes())

	data, err := marshalFn(cmd)
	if err != nil {
		build.ReleaseLoaders()

		return nil, fmt.Errorf("%w: %w", ErrMarshalProposal, err)
	}

	guardStart := time.Now()

	updatedPreloads, guard, err := p.AcquireProposalGuard(build)
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
		// Rare: next-index generation shifted between Build
		// and AcquireProposalGuard. The ExecutionPlan was rebuilt under
		// the proposal lock; re-assign bits + re-marshal the entire
		// Proposal because the rebuild changes preload boundaries and
		// possibly values.
		result.Rebuilt = true
		cmd.ExecutionPlan = updatedPreloads
		build.applyBits(cmd, updatedPreloads.GetAttributes())

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

// applyBits is the per-marshal hook that flows the proposal's final
// AttributeCoverage slice into every WriteOperation's coverage bitset. Run
// calls this once on the happy path and again on the rare rebuild
// under guard.
//
// The planLookupKey→position index is built ONCE per call and reused
// across every operation: a proposal's plans slice is identical for
// all operations in the batch, so rebuilding the map per operation
// costs O(N·P) runtime.mapassign for N orders × P plans where O(P)
// suffices.
func (b *BuildResult) applyBits(_ *raftcmdpb.Proposal, plans []*raftcmdpb.AttributeCoverage) {
	var (
		index     map[planLookupKey]uint32
		planCount = len(plans)
	)

	if planCount > 0 {
		index = buildPlanIndex(plans)
	}

	for _, op := range b.operations {
		if op.Target == nil {
			continue
		}

		if planCount == 0 {
			*op.Target = nil

			continue
		}

		*op.Target = bitsForNeedsWithIndex(op.Coverage, planCount, index)
	}
}

// runWithoutPreload is the no-preload fast path of Run. It fires when
// the aggregated needs are empty: there is nothing preloaded so no
// generation revalidation can be wrong, and AcquireProposalGuard would
// do nothing useful. The tracker mutex is held only long enough to
// inject PredictedIndex and push the proposal into Raft's queue.
//
// Returns a RunResult whose Guard wraps build.token so the caller's
// usual ReleaseLoaders() still works uniformly. build.token is typically
// the empty token from a zero-Coverage Build, so ReleaseLoaders is a no-op
// — but symmetry with the slow path keeps the call sites simple.
func (p *Builder) runWithoutPreload(
	ctx context.Context,
	cmd *raftcmdpb.Proposal,
	build *BuildResult,
	marshalFn func(*raftcmdpb.Proposal) ([]byte, error),
	proposer Proposer,
) (*RunResult, error) {
	// The empty-needs path reads nothing from the cache, so neither
	// the AttributeCoverage slice nor the CacheEpoch must ride on the
	// proposal — both would only trip checkStaleProposal on a
	// cluster-config cache reset between Build and apply. But we
	// keep IdempotencyKeys: machine.Preload applies them to the
	// IdempotencyStore on every node, and dropping them would let an
	// idempotent no-read order (maintenance, signing, etc.) apply
	// twice when the FSM's in-memory map does not already hold the
	// persisted duplicate.
	//
	// Read the keys from build.ExecutionPlan, not cmd.ExecutionPlan:
	// the slow path's `cmd.ExecutionPlan = build.ExecutionPlan`
	// assignment happens after the fast-path branch fires, so the
	// fresh keys Build just resolved live on build.ExecutionPlan
	// only. Reading cmd here would drop them and let a duplicate
	// idempotent order apply twice.
	if idem := build.ExecutionPlan.GetIdempotencyKeys(); len(idem) > 0 {
		cmd.ExecutionPlan = &raftcmdpb.ExecutionPlan{
			IdempotencyKeys: idem,
		}
	} else {
		cmd.ExecutionPlan = nil
	}

	build.applyBits(cmd, nil)

	data, err := marshalFn(cmd)
	if err != nil {
		build.ReleaseLoaders()

		return nil, fmt.Errorf("%w: %w", ErrMarshalProposal, err)
	}

	guard := &ProposalGuard{p: p, token: build.token}

	lockStart := time.Now()

	p.LockTracker()

	cmd.PredictedIndex = p.TrackerNext()
	data = AppendProposalPredictedIndex(data, cmd.GetPredictedIndex())

	proposal := node.NewProposal(cmd.GetId(), data)

	proposeStart := time.Now()
	fsmFuture, err := proposer.Propose(ctx, proposal)
	proposeDuration := time.Since(proposeStart)

	p.UnlockTracker()

	if err != nil {
		guard.ReleaseLoaders()

		return nil, err
	}

	return &RunResult{
		Proposal:         proposal,
		FSMFuture:        fsmFuture,
		Guard:            guard,
		LockHeldDuration: time.Since(lockStart),
		ProposeStartTime: proposeStart,
		ProposeDuration:  proposeDuration,
	}, nil
}
