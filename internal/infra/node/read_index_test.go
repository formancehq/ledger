package node

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v4/logging"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

func TestReadIndexOnLeader(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	cluster := NewCluster(t, 3, DefaultClusterConfig())
	_ = cluster.Start(ctx)

	leaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	leader := cluster.GetNodeByID(leaderID)
	require.NotNil(t, leader)

	// Create a ledger so there's some committed state
	_, err = createLedger(ctx, leader.Node, "test-ledger")
	require.NoError(t, err)

	// ReadIndex on leader should succeed
	err = leader.Node.ReadIndexAndWait(ctx)
	require.NoError(t, err)
}

func TestReadIndexOnFollower(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	cluster := NewCluster(t, 3, DefaultClusterConfig())
	_ = cluster.Start(ctx)

	leaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	leader := cluster.GetNodeByID(leaderID)
	require.NotNil(t, leader)

	// Find a follower
	var follower *ClusterNode

	for i := range cluster.Size() {
		n := cluster.GetNode(i)
		if n.ID != leaderID {
			follower = n

			break
		}
	}

	require.NotNil(t, follower)

	// Create a ledger on the leader
	_, err = createLedger(ctx, leader.Node, "test-ledger")
	require.NoError(t, err)

	// Wait for replication to the follower
	require.Eventually(t, func() bool {
		return listLedgerContains(follower.Store, "test-ledger")
	}, 5*time.Second, 100*time.Millisecond)

	// ReadIndex on follower should succeed
	err = follower.Node.ReadIndexAndWait(ctx)
	require.NoError(t, err)
}

func TestReadIndexLinearizability(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	cluster := NewCluster(t, 3, DefaultClusterConfig())
	_ = cluster.Start(ctx)

	leaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	leader := cluster.GetNodeByID(leaderID)
	require.NotNil(t, leader)

	// Find a follower
	var follower *ClusterNode

	for i := range cluster.Size() {
		n := cluster.GetNode(i)
		if n.ID != leaderID {
			follower = n

			break
		}
	}

	require.NotNil(t, follower)

	// Create multiple ledgers on the leader
	for i := range 5 {
		_, err := createLedger(ctx, leader.Node, fmt.Sprintf("ledger-%d", i))
		require.NoError(t, err)
	}

	// Immediately call ReadIndexAndWait on the follower.
	// After it returns, ALL writes committed before the ReadIndex call
	// must be visible in the follower's local store.
	err = follower.Node.ReadIndexAndWait(ctx)
	require.NoError(t, err)

	// Verify all ledgers are visible on the follower after the read barrier
	for i := range 5 {
		require.True(t, listLedgerContains(follower.Store, fmt.Sprintf("ledger-%d", i)),
			"ledger-%d should be visible on follower after ReadIndex", i)
	}
}

func TestReadIndexContextCancellation(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	cluster := NewCluster(t, 3, DefaultClusterConfig())
	_ = cluster.Start(ctx)

	leaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Find a follower and disconnect it
	var follower *ClusterNode

	for i := range cluster.Size() {
		n := cluster.GetNode(i)
		if n.ID != leaderID {
			follower = n

			break
		}
	}

	require.NotNil(t, follower)

	// Disconnect the follower so ReadIndex can't complete
	cluster.DisconnectNode(follower.ID)

	// ReadIndex with a short deadline should return context error
	shortCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	err = follower.Node.ReadIndexAndWait(shortCtx)
	require.Error(t, err)
	// The disconnected follower may either:
	// - lose track of the leader (heartbeat timeout) → ErrNoLeader
	// - still know the leader but fail to complete ReadIndex → DeadlineExceeded
	require.True(t,
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, commonpb.ErrNoLeader),
		"expected DeadlineExceeded or ErrNoLeader, got: %v", err,
	)

	// Reconnect for clean shutdown
	cluster.ReconnectNode(follower.ID)
}
