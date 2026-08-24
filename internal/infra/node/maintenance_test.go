package node

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// newTestMaintenanceNode builds a minimal *Node sufficient to exercise
// doMaintenance in isolation. The applier is not started — only its
// Store() and CompactionMargin() accessors are used by doMaintenance.
func newTestMaintenanceNode(t *testing.T) (*Node, *testApplierSetup) {
	t.Helper()

	setup := newTestApplierSetup(t)

	node := &Node{
		logger:     logging.Testing(),
		wal:        setup.wal,
		fsm:        setup.fsm,
		applier:    setup.applier,
		membership: newTestMembership(t),
	}
	node.confState.Store(setup.confState)

	return node, setup
}

// applyEntry feeds a single entry through the Applier synchronously by
// submitting, draining, and stopping the applier. After it returns,
// fsm.LastPersistedIndex reflects the entry.
func (s *testApplierSetup) applyEntry(t *testing.T, ctx context.Context, entry *raftpb.Entry) {
	t.Helper()

	runDone := make(chan error, 1)

	go func() {
		runDone <- s.applier.Run(ctx, s.stop)
	}()

	s.applier.Submit([]*raftpb.Entry{entry}, nil, nil, s.stop)
	s.applier.Drain(s.stop)

	close(s.stop)
	require.NoError(t, <-runDone)
}

// TestDoMaintenance_NoEntries_NoOp verifies that calling doMaintenance on a
// fresh node (no entries applied) does not create a WAL snapshot and does
// not advance lastCheckpointPersistedIndex. The early-skip guard keeps the
// maintenance loop free on idle clusters.
func TestDoMaintenance_NoEntries_NoOp(t *testing.T) {
	t.Parallel()

	node, _ := newTestMaintenanceNode(t)

	snapBefore, err := node.wal.Snapshot()
	require.NoError(t, err)

	require.NoError(t, node.doMaintenance())

	snapAfter, err := node.wal.Snapshot()
	require.NoError(t, err)

	require.Equal(t, snapBefore.GetMetadata().GetIndex(), snapAfter.GetMetadata().GetIndex(),
		"WAL snapshot must not advance when no entries are persisted")
	require.Zero(t, node.lastCheckpointPersistedIndex,
		"lastCheckpointPersistedIndex must not advance when nothing was checkpointed")
}

// TestDoMaintenance_AdvancesSnapshotAndCheckpoint verifies the happy path:
// after entries are persisted, doMaintenance creates a fresh Raft WAL
// snapshot at lastPersistedIndex and advances lastCheckpointPersistedIndex.
func TestDoMaintenance_AdvancesSnapshotAndCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	node, setup := newTestMaintenanceNode(t)

	entry, _ := makeCreateLedgerEntry(t, 1, "test-ledger")
	setup.applyEntry(t, ctx, entry)

	persisted := node.fsm.LastPersistedIndex()
	require.Equal(t, uint64(1), persisted)

	require.NoError(t, node.doMaintenance())

	snapAfter, err := node.wal.Snapshot()
	require.NoError(t, err)

	require.Equal(t, persisted, snapAfter.GetMetadata().GetIndex(),
		"WAL snapshot index must match the lastPersistedIndex captured by doMaintenance")
	require.Equal(t, persisted, node.lastCheckpointPersistedIndex,
		"lastCheckpointPersistedIndex must be updated after a successful Pebble checkpoint")
}

// TestDoMaintenance_SyncWALFailure_SkipsSnapshotAndCheckpoint exercises the
// safety property: if Pebble's WAL cannot be sync'd, doMaintenance must NOT
// create a Raft WAL snapshot (which would claim durability we don't have)
// and must NOT advance lastCheckpointPersistedIndex.
func TestDoMaintenance_SyncWALFailure_SkipsSnapshotAndCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	node, setup := newTestMaintenanceNode(t)

	entry, _ := makeCreateLedgerEntry(t, 1, "test-ledger")
	setup.applyEntry(t, ctx, entry)

	persisted := node.fsm.LastPersistedIndex()
	require.Equal(t, uint64(1), persisted)

	snapBefore, err := node.wal.Snapshot()
	require.NoError(t, err)

	// Force SyncWAL to fail by closing the store. ErrStoreClosed propagates
	// out of SyncWAL, doMaintenance must bail out without touching the WAL
	// snapshot or the checkpoint marker.
	require.NoError(t, setup.store.Close())

	require.NoError(t, node.doMaintenance())

	snapAfter, err := node.wal.Snapshot()
	require.NoError(t, err)

	require.Equal(t, snapBefore.GetMetadata().GetIndex(), snapAfter.GetMetadata().GetIndex(),
		"WAL snapshot must not advance when SyncWAL fails")
	require.Zero(t, node.lastCheckpointPersistedIndex,
		"lastCheckpointPersistedIndex must not advance when SyncWAL fails")
}

// TestBackgroundMaintenance_WALDirectoryMissing_StopsTheLoop exercises the one
// failure the next tick cannot retry: with the WAL directory gone, everything
// this node acknowledges as persisted is unrecoverable, so the maintenance loop
// must exit with the error — which stops the node — rather than log it and tick
// again.
func TestBackgroundMaintenance_WALDirectoryMissing_StopsTheLoop(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	node, setup := newTestMaintenanceNode(t)
	node.config.MaintenanceInterval = time.Millisecond

	entry, _ := makeCreateLedgerEntry(t, 1, "test-ledger")
	setup.applyEntry(t, ctx, entry)

	require.NoError(t, os.RemoveAll(setup.walDir))

	stop := make(chan struct{})
	exited := make(chan error, 1)

	go func() { exited <- node.runBackgroundMaintenance(ctx, stop) }()

	select {
	case err := <-exited:
		require.ErrorIs(t, err, wal.ErrWALDirectoryMissing)
	case <-time.After(10 * time.Second):
		close(stop)
		t.Fatal("maintenance loop kept ticking with the WAL directory gone")
	}
}
