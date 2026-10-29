package node

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
)

func TestProposeRejectsAfterDrainEnqueue(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.drainOutstanding = 1
	_, err := n.Propose(t.Context(), NewProposal(1, []byte("business")))
	require.ErrorIs(t, err, domain.ErrMaintenanceMode)
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
