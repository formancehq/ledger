package node

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

func TestHandleProposalAppliesAdmissionAtRaftBoundary(t *testing.T) {
	setup := newTestApplierSetup(t)
	rejected := errors.New("capability policy changed before Raft proposal")

	n := &Node{
		applier:      setup.applier,
		indexTracker: NewIndexTracker(10),
		proposalAdmission: func(context.Context, *Proposal) error {
			return rejected
		},
	}

	proposal := newProposal(42, []byte("proposal"), nil)
	fsmFuture := futures.New[state.ApplyResult]()
	setup.applier.StoreFuture(proposal.commandID, 1, fsmFuture)
	n.indexTracker.Increment(1)

	n.handleProposal(proposal)

	_, err := proposal.Wait(context.Background())
	require.ErrorIs(t, err, rejected)
	_, err = fsmFuture.Wait(context.Background())
	require.ErrorIs(t, err, rejected)
	require.Equal(t, uint64(10), n.indexTracker.Next())
}
