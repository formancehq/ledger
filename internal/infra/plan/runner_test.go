package plan

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/preload"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// stubFailingProposer rejects every Propose call. Used by tests that
// only care about Run's behaviour up to the proposer hand-off — past
// that point the runner unwinds the way the error path documents.
type stubFailingProposer struct{ err error }

func (s stubFailingProposer) Propose(_ context.Context, _ *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	return nil, s.err
}

// TestRunWithoutPreload_ClearsPreSetExecutionPlan pins the empty-needs
// fast path's contract: even if a caller pre-sets cmd.ExecutionPlan
// (admission does this unconditionally), runWithoutPreload must clear
// it so no Build-time CacheEpoch rides on a proposal that reads
// nothing. Without this, a cache reset between Build and apply would
// trip checkStaleProposal on a no-read order.
func TestRunWithoutPreload_ClearsPreSetExecutionPlan(t *testing.T) {
	t.Parallel()

	tracker := node.NewIndexTracker(1)
	builder := &Builder{tracker: tracker, loaders: preload.NewLoaders()}

	cmd := &raftcmdpb.Proposal{
		Id: 42,
		ExecutionPlan: &raftcmdpb.ExecutionPlan{
			CacheEpoch: 7, // a non-zero epoch that would otherwise survive into apply
		},
	}

	build := &BuildResult{
		ExecutionPlan: cmd.GetExecutionPlan(),
		aggregate:     NewNeeds(), // empty → triggers the fast path
		operations:    nil,
		token:         &preload.CleanupToken{},
	}

	marshalled := false
	marshalFn := func(p *raftcmdpb.Proposal) ([]byte, error) {
		marshalled = true
		require.Nil(t, p.GetExecutionPlan(), "ExecutionPlan must be cleared before marshal")

		return []byte{0}, nil
	}

	proposer := stubFailingProposer{err: errors.New("stub")}

	_, err := builder.Run(context.Background(), cmd, build, marshalFn, proposer)
	require.Error(t, err, "stub proposer should bubble its error")
	require.True(t, marshalled, "marshalFn must have run")
	require.Nil(t, cmd.GetExecutionPlan(), "cmd.ExecutionPlan must be nil after Run on the no-preload fast path")
}

// TestRun_IdempotencyOnlyNeedsTakesFastPath pins the AttributeKeysCount
// gate: a proposal that only carries idempotency keys (no cache
// attribute reads — e.g. maintenance / signing / chapter-schedule
// operations) must take runWithoutPreload, not the slow path. Without
// this, the slow path would serialize cache_epoch on cmd and a
// cluster-config reset between Build and apply would spuriously reject
// the proposal as stale even though it never consulted the cache.
//
// The fast path must also preserve ExecutionPlan.IdempotencyKeys so
// machine.Preload restores the IdempotencyStore on every node;
// dropping them would let the same idempotent order apply twice when
// the FSM's in-memory map does not already hold the persisted
// duplicate. Cache attributes / epoch are still cleared.
func TestRun_IdempotencyOnlyNeedsTakesFastPath(t *testing.T) {
	t.Parallel()

	tracker := node.NewIndexTracker(1)
	builder := &Builder{tracker: tracker, loaders: preload.NewLoaders()}

	// cmd starts with no ExecutionPlan (typical caller shape: a fresh
	// commands.NewCommand). Build is what populates the fresh
	// IdempotencyKeys; the fast path must pick them up from
	// build.ExecutionPlan, not from whatever cmd happens to carry.
	cmd := &raftcmdpb.Proposal{Id: 42}

	needs := NewNeeds()
	needs.IdempotencyKeys[domain.IdempotencyKey{Key: "idem-only"}] = struct{}{}

	build := &BuildResult{
		ExecutionPlan: &raftcmdpb.ExecutionPlan{
			CacheEpoch:      7, // would survive into apply on the slow path
			IdempotencyKeys: []*raftcmdpb.ReloadIdempotencyKey{{Key: "idem-only"}},
		},
		aggregate:  needs, // TotalKeys() == 1, but AttributeKeysCount() == 0
		operations: nil,
		token:      &preload.CleanupToken{},
	}

	marshalFn := func(p *raftcmdpb.Proposal) ([]byte, error) {
		ep := p.GetExecutionPlan()
		require.NotNil(t, ep, "ExecutionPlan must survive to carry IdempotencyKeys")
		require.Empty(t, ep.GetAttributes(), "Attributes must be dropped on the fast path")
		require.Zero(t, ep.GetCacheEpoch(), "CacheEpoch must be dropped on the fast path")
		require.Len(t, ep.GetIdempotencyKeys(), 1, "IdempotencyKeys must survive")
		require.Equal(t, "idem-only", ep.GetIdempotencyKeys()[0].GetKey())

		return []byte{0}, nil
	}

	proposer := stubFailingProposer{err: errors.New("stub")}

	_, err := builder.Run(context.Background(), cmd, build, marshalFn, proposer)
	require.Error(t, err, "stub proposer should bubble its error")

	ep := cmd.GetExecutionPlan()
	require.NotNil(t, ep)
	require.Empty(t, ep.GetAttributes())
	require.Zero(t, ep.GetCacheEpoch())
	require.Len(t, ep.GetIdempotencyKeys(), 1)
}
