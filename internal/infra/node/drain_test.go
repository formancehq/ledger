package node

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
)

func TestProposeRejectsAfterDrainEnqueue(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.drainEnqueued = true
	_, err := n.Propose(t.Context(), NewProposal(1, []byte("business")))
	require.ErrorIs(t, err, domain.ErrMaintenanceMode)
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
