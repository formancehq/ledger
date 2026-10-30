package node

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// pendingLeaderReady installs a readiness generation that completes only when
// cancelled: the leader's FSM has not caught up.
func pendingLeaderReady(t *testing.T, n *Node) *leaderReadyGeneration {
	t.Helper()

	generation := newLeaderReadyGeneration(context.Background())
	n.leaderReady.Store(generation)

	go func() {
		_ = generation.run(
			func(ctx context.Context) error {
				<-ctx.Done()

				return ctx.Err()
			},
			func() { t.Error("cancelled generation emitted readiness") },
		)
	}()

	return generation
}

// TestAddLearnerWaitsForLeaderReady pins that membership admission never
// reaches the proposal path while the leader's FSM is still catching up: the
// removed-member registry it checks lives in the store, which a leader elected
// while out of sync has not restored yet.
func TestAddLearnerWaitsForLeaderReady(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		n := newConfiguredPeersTestNode(t)
		n.logger = logging.Testing()
		generation := pendingLeaderReady(t, n)

		results := make(chan error, 1)
		go func() {
			results <- n.AddLearner(t.Context(), 2, "new:7777", "new:8888", []byte("next-instance-id"))
		}()

		// The command channel is unbuffered: an admission that reached it would
		// be parked on the send, so a receive here would succeed.
		time.Sleep(time.Minute)
		synctest.Wait()
		select {
		case <-n.clusterCommandCh:
			t.Fatal("admission reached the proposal path before leader readiness")
		default:
		}

		generation.cancelAndWait()
		require.ErrorIs(t, receiveForceRemoveError(t, results), ErrNotLeader)
	})
}

// TestCheckAndPromoteLearnersWaitsForLeaderReady pins the same gate on
// automatic promotion, which runs on the orchestrate loop and skips its tick
// instead of blocking.
func TestCheckAndPromoteLearnersWaitsForLeaderReady(t *testing.T) {
	t.Parallel()

	n := newConfiguredPeersTestNode(t)
	n.logger = logging.Testing()
	n.lastAutoPromote = make(map[uint64]time.Time)
	generation := pendingLeaderReady(t, n)
	defer generation.cancelAndWait()

	ack := msgWithIndex(raftpb.MsgAppResp, 2, 1, 1)
	ack.Term = new(n.rawNode.Status().GetTerm())
	require.NoError(t, n.rawNode.Step(ack))
	require.Equal(t, uint64(1), n.rawNode.Status().Progress[2].Match)

	require.NoError(t, n.checkAndPromoteLearners())

	_, attempted := n.lastAutoPromote[2]
	require.False(t, attempted, "promotion attempted before leader readiness")
	if n.rawNode.HasReady() {
		for _, entry := range n.rawNode.Ready().Entries {
			require.NotEqual(t, raftpb.EntryConfChangeV2, entry.GetType(), "promotion proposed before leader readiness")
		}
	}
}
