package node

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestCompactionFenceRequiresAQuorum(t *testing.T) {
	t.Parallel()

	node, setup := newTestMaintenanceNode(t)
	node.confState.Store(&raftpb.ConfState{Voters: []uint64{1, 2, 3}})
	entry, _ := makeCreateLedgerEntry(t, 1, "test-ledger")
	setup.applyEntry(t, context.Background(), entry)

	require.Zero(t, node.CompactionFence(), "an unknown voter must block compaction")
	node.RecordPeerPersistedIndex(2, 1)
	require.Equal(t, uint64(1), node.CompactionFence(), "the local node and one remote voter form a quorum")
	node.RecordPeerPersistedIndex(3, 1)
	require.Equal(t, uint64(1), node.CompactionFence())

	// Reports are not monotonic: a peer can restart from an older checkpoint.
	node.RecordPeerPersistedIndex(2, 0)
	require.Equal(t, uint64(1), node.CompactionFence(), "the local node and voter 3 still form a quorum")
	node.RecordPeerPersistedIndex(3, 0)
	require.Zero(t, node.CompactionFence())
}

func TestCompactionFenceResetForgetsReports(t *testing.T) {
	t.Parallel()

	node, setup := newTestMaintenanceNode(t)
	node.confState.Store(&raftpb.ConfState{Voters: []uint64{1, 2}})
	entry, _ := makeCreateLedgerEntry(t, 1, "test-ledger")
	setup.applyEntry(t, context.Background(), entry)
	node.RecordPeerPersistedIndex(2, 1)
	require.Equal(t, uint64(1), node.CompactionFence())

	node.ResetPeerPersistedIndexes()
	require.Zero(t, node.CompactionFence())
}

func TestCompactionFenceRequiresQuorumsDuringJointConsensus(t *testing.T) {
	t.Parallel()

	node, setup := newTestMaintenanceNode(t)
	node.confState.Store(&raftpb.ConfState{
		Voters:         []uint64{1, 2},
		VotersOutgoing: []uint64{1, 3},
	})
	entry, _ := makeCreateLedgerEntry(t, 1, "test-ledger")
	setup.applyEntry(t, context.Background(), entry)

	node.RecordPeerPersistedIndex(2, 1)
	require.Zero(t, node.CompactionFence(), "the outgoing configuration still lacks a quorum")
	node.RecordPeerPersistedIndex(3, 1)
	require.Equal(t, uint64(1), node.CompactionFence())
}

func TestLearnerPromotionRequiresDurableSnapshotCoverage(t *testing.T) {
	t.Parallel()

	node, setup := newTestMaintenanceNode(t)
	entry, _ := makeCreateLedgerEntry(t, 1, "test-ledger")
	setup.applyEntry(t, context.Background(), entry)
	require.NoError(t, node.wal.CreateSnapshot(1, node.confState.Load(), nil))

	covered, err := node.learnerCoversWALSnapshot(2)
	require.NoError(t, err)
	require.False(t, covered, "Raft replication alone does not prove learner store durability")

	node.RecordPeerPersistedIndex(2, 0)
	covered, err = node.learnerCoversWALSnapshot(2)
	require.NoError(t, err)
	require.False(t, covered)

	node.RecordPeerPersistedIndex(2, 1)
	covered, err = node.learnerCoversWALSnapshot(2)
	require.NoError(t, err)
	require.True(t, covered)
}
