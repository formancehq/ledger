package node

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProposeAppliesAdmissionBeforeIndexReservation(t *testing.T) {
	rejected := errors.New("capability policy changed before Raft proposal")

	n := &Node{
		indexTracker: NewIndexTracker(10),
		proposalAdmission: func(context.Context, *Proposal) error {
			return rejected
		},
	}

	proposal := newProposal(42, []byte("proposal"), nil)
	_, err := n.Propose(context.Background(), proposal)
	require.ErrorIs(t, err, rejected)
	require.Equal(t, uint64(10), n.indexTracker.Next())
}

func TestProposeRejectsLegacyAccountWriteWhileActivationIsPending(t *testing.T) {
	called := false
	n := &Node{
		indexTracker:                  NewIndexTracker(10),
		pendingIncarnationActivations: 1,
		proposalAdmission: func(context.Context, *Proposal) error {
			called = true
			return nil
		},
	}
	proposal := newProposal(42, []byte("proposal"), nil)
	proposal.touchesAccounts = true
	_, err := n.Propose(context.Background(), proposal)
	require.ErrorContains(t, err, "pending incarnation capability activation")
	require.False(t, called)
	require.Equal(t, uint64(10), n.indexTracker.Next())
}
