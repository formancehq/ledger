package node

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
)

// requireCompletePromotion inspects the actual entry emitted by RawNode, so
// each production caller must propagate the payload through ProposeConfChange.
func requireCompletePromotion(t *testing.T, n *Node) (*raftpb.Entry, *raftpb.ConfChangeV2) {
	t.Helper()

	ready := n.rawNode.Ready()
	var promotions []*raftpb.Entry
	for _, entry := range ready.Entries {
		if entry.GetType() == raftpb.EntryConfChangeV2 {
			promotions = append(promotions, entry)
		}
	}
	require.Len(t, promotions, 1, "the production trigger must propose one promotion")
	entry := promotions[0]
	cc, ok, err := membership.UnmarshalConfChangeV2(entry)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, cc.GetChanges(), 1)
	require.Equal(t, raftpb.ConfChangeAddNode, cc.GetChanges()[0].GetType())
	require.Equal(t, uint64(2), cc.GetChanges()[0].GetNodeId())
	require.NoError(t, membership.ValidateConfChangeIdentities(cc))
	payload, err := membership.UnmarshalConfChangeContext(cc.GetContext())
	require.NoError(t, err)
	require.Equal(t, "old:7777", payload.RaftAddress)
	require.Equal(t, "old:8888", payload.ServiceAddress)
	require.Equal(t, []byte("peer-instance-id"), payload.InstanceID)

	return entry, cc
}

func TestPromoteLearner_ProposesCompleteRegisteredIdentity(t *testing.T) {
	t.Parallel()

	n := newConfiguredPeersTestNode(t)
	n.logger = logging.Testing()
	results := make(chan error, 1)
	go func() {
		results <- n.PromoteLearner(t.Context(), 2)
	}()

	cmd := <-n.clusterCommandCh
	err := cmd.fn()
	require.NoError(t, err)
	entry, cc := requireCompletePromotion(t, n)
	payload, err := membership.UnmarshalConfChangeContext(cc.GetContext())
	require.NoError(t, err)
	require.NotEmpty(t, payload.ProposalID, "manual promotion must retain waiter correlation")

	// Acknowledge the captured entry through the same correlation mechanism
	// as finishReady, then release the command response and its caller.
	pending, err := n.takePendingConfChange(cc, entry.GetIndex())
	require.NoError(t, err)
	require.NotNil(t, pending)
	pending.future.Resolve(pending.index, nil)
	cmd.errCh <- nil
	require.NoError(t, <-results)
}

func TestCheckAndPromoteLearners_ProposesCompleteRegisteredIdentity(t *testing.T) {
	t.Parallel()

	n := newConfiguredPeersTestNode(t)
	n.logger = logging.Testing()
	n.lastAutoPromote = make(map[uint64]time.Time)

	// A real replication acknowledgement makes the learner active and caught
	// up to the leader's initial no-op entry, satisfying the automatic gate.
	ack := msgWithIndex(raftpb.MsgAppResp, 2, 1, 1)
	ack.Term = new(n.rawNode.Status().GetTerm())
	require.NoError(t, n.rawNode.Step(ack))
	progress := n.rawNode.Status().Progress[2]
	require.True(t, progress.IsLearner)
	require.True(t, progress.RecentActive)
	require.Equal(t, uint64(1), progress.Match)

	require.NoError(t, n.checkAndPromoteLearners())
	_, cc := requireCompletePromotion(t, n)
	payload, err := membership.UnmarshalConfChangeContext(cc.GetContext())
	require.NoError(t, err)
	require.Empty(t, payload.ProposalID, "automatic promotion has no synchronous waiter")
	_, attempted := n.lastAutoPromote[2]
	require.True(t, attempted)
}
