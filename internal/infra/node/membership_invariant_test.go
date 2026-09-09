package node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestLeaderMembershipOperationsRejectMissingRowBeforeMutation(t *testing.T) {
	t.Parallel()

	for name, invoke := range map[string]func(context.Context, *Node) error{
		"add learner": func(ctx context.Context, n *Node) error {
			return n.AddLearner(ctx, 2, "peer:7777", "peer:8888", []byte("next-instance-id"))
		},
		"remove node": func(ctx context.Context, n *Node) error {
			return n.RemoveNode(ctx, 2)
		},
		"force remove node": func(ctx context.Context, n *Node) error {
			return n.ForceRemoveNode(ctx, 2)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			n := newConfiguredPeersTestNode(t)
			n.logger = logging.Testing()
			n.config.NodeID = 1
			n.confState.Store(&raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}})
			n.membership.Remove(2)
			before := n.rawNode.Status()
			require.Contains(t, before.Progress, uint64(2), "reach the missing-row guard for a configured member")

			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			results := make(chan error, 1)
			go func() { results <- invoke(ctx, n) }()
			select {
			case cmd := <-n.clusterCommandCh:
				cmd.errCh <- cmd.fn()
			case <-ctx.Done():
				t.Fatal("membership operation did not reach orchestrate")
			}
			require.ErrorContains(t, receiveForceRemoveError(t, results), "invariant: raft member 2 has no membership row")
			require.Equal(t, before, n.rawNode.Status(), "rejection must preserve configuration and replication progress")
			require.False(t, n.rawNode.HasReady(), "rejection must not stage a ConfChange proposal")
			require.Equal(t, []uint64{2}, n.confState.Load().GetLearners())
			require.NoError(t, n.terminalError(), "pre-mutation invariant rejection is returned to the caller")
		})
	}
}

func TestCheckAndPromoteLearnersRejectsMissingRowBeforeAttempt(t *testing.T) {
	t.Parallel()

	n := newConfiguredPeersTestNode(t)
	n.logger = logging.Testing()
	n.lastAutoPromote = make(map[uint64]time.Time)
	n.membership.Remove(2)
	ack := msgWithIndex(raftpb.MsgAppResp, 2, 1, 1)
	ack.Term = new(n.rawNode.Status().GetTerm())
	require.NoError(t, n.rawNode.Step(ack))
	before := n.rawNode.Status()
	require.True(t, before.Progress[2].RecentActive)
	require.Equal(t, uint64(1), before.Progress[2].Match)

	require.ErrorContains(t, n.checkAndPromoteLearners(), "invariant: learner 2 has no membership row")
	require.Equal(t, before, n.rawNode.Status())
	// promotionContext has a later same-shaped guard. Checking the attempt
	// marker pins rejection before that fallback and before admission work.
	require.NotContains(t, n.lastAutoPromote, uint64(2))
}
