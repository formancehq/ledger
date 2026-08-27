package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

func correlatedConfChange(t *testing.T, proposalID string, nodeID uint64, changeType raftpb.ConfChangeType) *raftpb.ConfChangeV2 {
	t.Helper()

	ccCtx, err := membership.MarshalConfChangeContext(membership.ConfChangeContext{
		ProposalID: proposalID,
	})
	require.NoError(t, err)

	return &raftpb.ConfChangeV2{
		Changes: []*raftpb.ConfChangeSingle{{
			Type:   new(changeType),
			NodeId: new(nodeID),
		}},
		Context: ccCtx,
	}
}

func TestTakePendingConfChangeUsesProposalIDAndChangeType(t *testing.T) {
	t.Parallel()

	n := &Node{}
	future := futures.New[uint64]()
	pending := &pendingConfChange{
		nodeID:        3,
		expectedTypes: []raftpb.ConfChangeType{raftpb.ConfChangeRemoveNode},
		future:        future,
	}
	n.pendingConfChanges.Store("new-removal", pending)

	staleRemoval := correlatedConfChange(t, "stale-removal", 3, raftpb.ConfChangeRemoveNode)
	resolved, err := n.takePendingConfChange(staleRemoval, 41)
	require.NoError(t, err)
	require.Nil(t, resolved, "an older proposal must not resolve a newer waiter for the same node")

	stillPending, ok := n.pendingConfChanges.Load("new-removal")
	require.True(t, ok)
	require.Same(t, pending, stillPending)

	matchingRemoval := correlatedConfChange(t, "new-removal", 3, raftpb.ConfChangeRemoveNode)
	resolved, err = n.takePendingConfChange(matchingRemoval, 42)
	require.NoError(t, err)
	require.NotNil(t, resolved)
	require.Same(t, future, resolved.future)
	require.Equal(t, uint64(42), resolved.index)

	_, ok = n.pendingConfChanges.Load("new-removal")
	require.False(t, ok)
}

func TestTakePendingConfChangeRejectsMismatchedChange(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		nodeID     uint64
		changeType raftpb.ConfChangeType
	}{
		"node ID": {
			nodeID:     4,
			changeType: raftpb.ConfChangeRemoveNode,
		},
		"change type": {
			nodeID:     3,
			changeType: raftpb.ConfChangeAddNode,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			n := &Node{}
			pending := &pendingConfChange{
				nodeID:        3,
				expectedTypes: []raftpb.ConfChangeType{raftpb.ConfChangeRemoveNode},
				future:        futures.New[uint64](),
			}
			n.pendingConfChanges.Store("remove-7", pending)

			wrongChange := correlatedConfChange(t, "remove-7", test.nodeID, test.changeType)
			resolved, err := n.takePendingConfChange(wrongChange, 42)
			require.Nil(t, resolved)
			require.ErrorContains(t, err, "unexpected change")

			stillPending, ok := n.pendingConfChanges.Load("remove-7")
			require.True(t, ok)
			require.Same(t, pending, stillPending)
		})
	}
}

func TestTakePendingConfChangeRejectsCorrelatedBatch(t *testing.T) {
	t.Parallel()

	cc := correlatedConfChange(t, "batched-change", 3, raftpb.ConfChangeRemoveNode)
	secondNodeID := uint64(4)
	cc.Changes = append(cc.Changes, &raftpb.ConfChangeSingle{
		Type:   new(raftpb.ConfChangeAddNode),
		NodeId: &secondNodeID,
	})

	resolved, err := (&Node{}).takePendingConfChange(cc, 42)
	require.Nil(t, resolved)
	require.ErrorContains(t, err, "must contain exactly one")
}
