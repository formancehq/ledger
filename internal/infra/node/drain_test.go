package node

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
)

func TestProposeRejectsAfterDrainEnqueue(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.drainOutstanding = 1
	_, err := n.Propose(t.Context(), newProposal(1, []byte("business"), nil))
	require.ErrorIs(t, err, domain.ErrMaintenanceMode)
}

func TestDrainProposalRejectsPreflightFromPriorLeadershipTerm(t *testing.T) {
	t.Parallel()
	n, setup := newTestMaintenanceNode(t)
	rn, err := raft.NewRawNode(&raft.Config{
		ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: setup.wal,
		MaxSizePerMsg: 1024 * 1024, MaxInflightMsgs: 256,
		Logger: NewLoggerAdapter(logging.Testing()),
	})
	require.NoError(t, err)
	n.rawNode = rn
	n.logger = logging.Testing()
	n.proposeCh = make(chan *Proposal, 1)
	n.indexTracker = NewIndexTracker(1)
	require.NoError(t, rn.Campaign())
	for rn.HasReady() {
		ready := rn.Ready()
		require.NoError(t, setup.wal.Append(ready.HardState, ready.Entries))
		rn.Advance(ready)
	}
	preflightTerm := rn.Status().GetTerm()
	require.Equal(t, raft.StateLeader, rn.Status().RaftState)

	// The original preflight captured member 1. A higher-term leader takes
	// over, adds member 2, then this node wins leadership again.
	require.NoError(t, rn.Step(&raftpb.Message{
		Type: new(raftpb.MsgHeartbeat), From: new(uint64(2)), To: new(uint64(1)),
		Term: new(preflightTerm + 1),
	}))
	rn.ApplyConfChange(&raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{{
		Type: new(raftpb.ConfChangeAddLearnerNode), NodeId: new(uint64(2)),
	}}})
	require.NoError(t, rn.Campaign())
	for rn.HasReady() {
		ready := rn.Ready()
		require.NoError(t, setup.wal.Append(ready.HardState, ready.Entries))
		rn.Advance(ready)
	}
	require.Equal(t, raft.StateLeader, rn.Status().RaftState)
	require.Greater(t, rn.Status().GetTerm(), preflightTerm)
	require.Contains(t, rn.Status().Progress, uint64(2))
	require.False(t, rn.HasReady())

	marker := newProposal(1, []byte("stale drain marker for member 1"), nil)
	marker.drainMarker = true
	marker.expectedTerm = preflightTerm
	n.indexTracker.Lock()
	future, err := n.Propose(t.Context(), marker)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	require.Same(t, marker, <-n.proposeCh)
	n.handleProposal(marker)
	_, err = marker.Wait(t.Context())
	require.ErrorIs(t, err, ErrLeadershipLost)
	_, err = future.Wait(t.Context())
	require.ErrorIs(t, err, ErrLeadershipLost)
	require.False(t, n.drainActiveOrPending())
	require.False(t, rn.HasReady(), "stale marker must not enter the Raft log")

	release := newProposal(2, []byte("stale release marker"), nil)
	release.releaseMarker = true
	release.expectedTerm = preflightTerm
	n.indexTracker.Lock()
	releaseFuture, err := n.Propose(t.Context(), release)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	require.Same(t, release, <-n.proposeCh)
	n.handleProposal(release)
	_, err = release.Wait(t.Context())
	require.ErrorIs(t, err, ErrLeadershipLost)
	_, err = releaseFuture.Wait(t.Context())
	require.ErrorIs(t, err, ErrLeadershipLost)
	require.False(t, rn.HasReady(), "stale release must not enter the Raft log")
}

func TestMembershipFenceOutlivesTimedOutDrainCaller(t *testing.T) {
	t.Parallel()
	n, setup := newTestMaintenanceNode(t)
	n.proposeCh = make(chan *Proposal, 1)
	n.indexTracker = NewIndexTracker(1)
	marker := newProposal(1, []byte("drain marker"), nil)
	marker.drainMarker = true
	n.indexTracker.Lock()
	future, err := n.Propose(t.Context(), marker)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = future.Wait(canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, n.drainActiveOrPending(), "membership must stay fenced after caller cancellation")
	n.applier.ResolveDroppedFuture(marker.commandID, ErrNotLeader)
	require.False(t, n.drainActiveOrPending(), "definitive drop releases the fence")
	n.drainOutstanding = 1
	setup.sharedState.SetDrainReleased(7)
	require.False(t, n.drainActiveOrPending(), "applied release supersedes an old latch")
}

func TestStableDrainMembershipWaitsForConfChangeAndHonorsCancellation(t *testing.T) {
	t.Parallel()
	n := &Node{}
	n.confChangeMu.Lock()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := n.WithStableDrainMembership(ctx, func() error {
		t.Fatal("callback ran before membership change completed")

		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	n.confChangeMu.Unlock()
}

func TestReleaseMarkerCanCrossClosedAdmission(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.proposeCh = make(chan *Proposal, 1)
	n.indexTracker = NewIndexTracker(1)
	n.drainOutstanding = 1
	release := newProposal(1, []byte("release"), nil)
	release.releaseMarker = true
	n.indexTracker.Lock()
	_, err := n.Propose(t.Context(), release)
	n.indexTracker.Unlock()
	require.NoError(t, err)
}

func TestDroppedDrainMarkerReopensAdmission(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.proposeCh = make(chan *Proposal, 3)
	n.indexTracker = NewIndexTracker(1)
	first := newProposal(1, []byte("first marker"), nil)
	first.drainMarker = true
	n.indexTracker.Lock()
	_, err := n.Propose(t.Context(), first)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	second := newProposal(2, []byte("second marker"), nil)
	second.drainMarker = true
	n.indexTracker.Lock()
	secondFuture, err := n.Propose(t.Context(), second)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	n.applier.ResolveDroppedFuture(first.commandID, ErrNotLeader)
	_, err = n.Propose(t.Context(), newProposal(1, []byte("still blocked"), nil))
	require.ErrorIs(t, err, domain.ErrMaintenanceMode)
	// The original RPC may have timed out. The applier still owns the
	// callback, so a later-term truncation releases the remaining slot.
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = secondFuture.Wait(canceled)
	require.ErrorIs(t, err, context.Canceled)
	n.applier.FailFuturesBelowTerm(1, ErrLeadershipLost)
	_, err = secondFuture.Wait(t.Context())
	require.ErrorIs(t, err, ErrLeadershipLost)
	n.indexTracker.Lock()
	_, err = n.Propose(t.Context(), newProposal(3, []byte("admitted after definitive drops"), nil))
	n.indexTracker.Unlock()
	require.NoError(t, err)
}

func TestRejectedQueuedDrainMarkerReopensAdmission(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.proposeCh = make(chan *Proposal, 2)
	n.indexTracker = NewIndexTracker(1)
	marker := newProposal(1, []byte("drain marker"), nil)
	marker.drainMarker = true
	n.indexTracker.Lock()
	_, err := n.Propose(t.Context(), marker)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	require.Same(t, marker, <-n.proposeCh)
	n.rejectQueuedProposal(marker, ErrNotLeader)
	n.indexTracker.Lock()
	_, err = n.Propose(t.Context(), newProposal(2, []byte("business after drop"), nil))
	n.indexTracker.Unlock()
	require.NoError(t, err)
}

func TestDrainEnqueueOrdersInFlightProposals(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.proposeCh = make(chan *Proposal, 3)
	n.indexTracker = NewIndexTracker(1)
	pre := newProposal(1, []byte("already admitted"), nil)
	n.indexTracker.Lock()
	_, err := n.Propose(t.Context(), pre)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	marker := newProposal(2, []byte("drain marker"), nil)
	marker.drainMarker = true
	n.indexTracker.Lock()
	_, err = n.Propose(t.Context(), marker)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	_, err = n.Propose(t.Context(), newProposal(3, []byte("later write"), nil))
	require.ErrorIs(t, err, domain.ErrMaintenanceMode)
	require.Same(t, pre, <-n.proposeCh)
	require.Same(t, marker, <-n.proposeCh)
	require.Empty(t, n.proposeCh)
}

func TestOldDrainDropCannotOpenNewEpoch(t *testing.T) {
	t.Parallel()
	n, setup := newTestMaintenanceNode(t)
	n.proposeCh = make(chan *Proposal, 3)
	n.indexTracker = NewIndexTracker(1)
	old := newProposal(1, []byte("old marker"), nil)
	old.drainMarker = true
	n.indexTracker.Lock()
	_, err := n.Propose(t.Context(), old)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	setup.sharedState.SetDrainReleased(7)
	newMarker := newProposal(2, []byte("new marker"), nil)
	newMarker.drainMarker = true
	n.indexTracker.Lock()
	_, err = n.Propose(t.Context(), newMarker)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	n.applier.ResolveDroppedFuture(old.commandID, ErrNotLeader)
	_, err = n.Propose(t.Context(), newProposal(3, []byte("must stay closed"), nil))
	require.ErrorIs(t, err, domain.ErrMaintenanceMode)
	n.applier.ResolveDroppedFuture(newMarker.commandID, ErrNotLeader)
	n.indexTracker.Lock()
	_, err = n.Propose(t.Context(), newProposal(4, []byte("new marker dropped"), nil))
	n.indexTracker.Unlock()
	require.NoError(t, err)
}
