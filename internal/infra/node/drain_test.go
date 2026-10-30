package node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestDrainProducersEmitNativeTechnicalPayloads(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*Node)
		propose func(context.Context, *Node) error
		check   func(*testing.T, *raftcmdpb.TechnicalUpdate)
	}{
		{
			name: "marker",
			propose: func(ctx context.Context, n *Node) error {
				_, err := n.BeginDrain(ctx, "epoch", []uint64{1, 2}, 0)
				return err
			},
			check: func(t *testing.T, update *raftcmdpb.TechnicalUpdate) {
				require.Equal(t, "epoch", update.GetDrainMarker().GetEpoch())
				require.Equal(t, []uint64{1, 2}, update.GetDrainMarker().GetMembers())
			},
		},
		{
			name: "release",
			prepare: func(n *Node) {
				n.fsm.State.DrainState = &raftcmdpb.DrainState{Epoch: "epoch"}
			},
			propose: func(ctx context.Context, n *Node) error {
				_, err := n.EndDrain(ctx, "epoch", 0)
				return err
			},
			check: func(t *testing.T, update *raftcmdpb.TechnicalUpdate) {
				require.Equal(t, "epoch", update.GetDrainRelease().GetEpoch())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, _ := newTestMaintenanceNode(t)
			n.proposeCh = make(chan *Proposal, 1)
			n.indexTracker = NewIndexTracker(1)
			if tc.prepare != nil {
				tc.prepare(n)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- tc.propose(ctx, n) }()

			var proposal *Proposal
			select {
			case proposal = <-n.proposeCh:
			case <-time.After(5 * time.Second):
				t.Fatal("drain producer did not enqueue a proposal")
			}
			var wire raftcmdpb.Proposal
			require.NoError(t, wire.UnmarshalVT(proposal.Data()))
			require.NotNil(t, wire.GetBatch())
			require.Empty(t, wire.GetOrders())
			require.Empty(t, wire.GetTechnicalUpdates())
			require.Len(t, wire.GetBatch().GetTechnicalUpdates(), 1)
			tc.check(t, wire.GetBatch().GetTechnicalUpdates()[0])
			cancel()
			require.ErrorIs(t, <-done, context.Canceled)
		})
	}
}

func TestProposeRejectsAfterDrainEnqueue(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.drainOutstanding = 1
	_, err := n.Propose(t.Context(), NewProposal(1, []byte("business")))
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

	marker := NewProposal(1, []byte("stale drain marker for member 1"))
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

	release := NewProposal(2, []byte("stale release marker"))
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
	marker := NewProposal(1, []byte("drain marker"))
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
	release := NewProposal(1, []byte("release"))
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
	first := NewProposal(1, []byte("first marker"))
	first.drainMarker = true
	n.indexTracker.Lock()
	_, err := n.Propose(t.Context(), first)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	second := NewProposal(2, []byte("second marker"))
	second.drainMarker = true
	n.indexTracker.Lock()
	secondFuture, err := n.Propose(t.Context(), second)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	n.applier.ResolveDroppedFuture(first.commandID, ErrNotLeader)
	_, err = n.Propose(t.Context(), NewProposal(1, []byte("still blocked")))
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
	_, err = n.Propose(t.Context(), NewProposal(3, []byte("admitted after definitive drops")))
	n.indexTracker.Unlock()
	require.NoError(t, err)
}

func TestRejectedQueuedDrainMarkerReopensAdmission(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.proposeCh = make(chan *Proposal, 2)
	n.indexTracker = NewIndexTracker(1)
	marker := NewProposal(1, []byte("drain marker"))
	marker.drainMarker = true
	n.indexTracker.Lock()
	_, err := n.Propose(t.Context(), marker)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	require.Same(t, marker, <-n.proposeCh)
	n.rejectQueuedProposal(marker, ErrNotLeader)
	n.indexTracker.Lock()
	_, err = n.Propose(t.Context(), NewProposal(2, []byte("business after drop")))
	n.indexTracker.Unlock()
	require.NoError(t, err)
}

func TestDrainEnqueueOrdersInFlightProposals(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.proposeCh = make(chan *Proposal, 3)
	n.indexTracker = NewIndexTracker(1)
	pre := NewProposal(1, []byte("already admitted"))
	n.indexTracker.Lock()
	_, err := n.Propose(t.Context(), pre)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	marker := NewProposal(2, []byte("drain marker"))
	marker.drainMarker = true
	n.indexTracker.Lock()
	_, err = n.Propose(t.Context(), marker)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	_, err = n.Propose(t.Context(), NewProposal(3, []byte("later write")))
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
	old := NewProposal(1, []byte("old marker"))
	old.drainMarker = true
	n.indexTracker.Lock()
	_, err := n.Propose(t.Context(), old)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	setup.sharedState.SetDrainReleased(7)
	newMarker := NewProposal(2, []byte("new marker"))
	newMarker.drainMarker = true
	n.indexTracker.Lock()
	_, err = n.Propose(t.Context(), newMarker)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	n.applier.ResolveDroppedFuture(old.commandID, ErrNotLeader)
	_, err = n.Propose(t.Context(), NewProposal(3, []byte("must stay closed")))
	require.ErrorIs(t, err, domain.ErrMaintenanceMode)
	n.applier.ResolveDroppedFuture(newMarker.commandID, ErrNotLeader)
	n.indexTracker.Lock()
	_, err = n.Propose(t.Context(), NewProposal(4, []byte("new marker dropped")))
	n.indexTracker.Unlock()
	require.NoError(t, err)
}
