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
