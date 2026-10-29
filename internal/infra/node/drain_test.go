package node

import (
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

func TestDroppedDrainMarkerReopensAdmission(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.proposeCh = make(chan *Proposal, 2)
	n.indexTracker = NewIndexTracker(1)
	n.drainOutstanding = 2
	n.releaseDroppedDrain()
	_, err := n.Propose(t.Context(), NewProposal(1, []byte("still blocked")))
	require.ErrorIs(t, err, domain.ErrMaintenanceMode)
	n.releaseDroppedDrain()
	n.indexTracker.Lock()
	_, err = n.Propose(t.Context(), NewProposal(2, []byte("admitted after definitive drops")))
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
