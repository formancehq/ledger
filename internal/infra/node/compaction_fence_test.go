package node

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestCompactionFenceRequiresEveryRemoteVoter(t *testing.T) {
	t.Parallel()

	node, setup := newTestMaintenanceNode(t)
	node.confState.Store(&raftpb.ConfState{Voters: []uint64{1, 2, 3}})
	entry, _ := makeCreateLedgerEntry(t, 1, "test-ledger")
	setup.applyEntry(t, context.Background(), entry)

	require.Zero(t, node.CompactionFence(), "an unknown voter must block compaction")
	node.RecordPeerPersistedIndex(2, 1)
	require.Zero(t, node.CompactionFence(), "the second voter is still unknown")
	node.RecordPeerPersistedIndex(3, 1)
	require.Equal(t, uint64(1), node.CompactionFence())

	// Reports are not monotonic: a peer can restart from an older checkpoint.
	node.RecordPeerPersistedIndex(2, 0)
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
