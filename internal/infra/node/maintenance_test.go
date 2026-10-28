package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

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

// TestDoMaintenanceRetainsEntriesUntilRaftApplyAcked reproduces the election
// crash reported by EN-3134: the FSM can be durable several entries ahead of
// RawNode while the apply response is queued. Compaction must follow the
// acknowledged Raft cursor, not the FSM cursor, or the election scan of the
// unapplied interval returns raft.ErrCompacted.
func TestDoMaintenanceRetainsEntriesUntilRaftApplyAcked(t *testing.T) {
	t.Parallel()

	node, setup := newTestMaintenanceNode(t)
	setup.applier.compactionMargin = 1

	entries := make([]*raftpb.Entry, 4)
	for i := range entries {
		entries[i], _ = makeCreateLedgerEntry(t, uint64(i+1), "maintenance-ledger-"+string(rune('a'+i)))
	}
	require.NoError(t, node.wal.Append(&raftpb.HardState{
		Term:   proto.Uint64(1),
		Vote:   proto.Uint64(1),
		Commit: proto.Uint64(4),
	}, entries))

	// Use a real RawNode so this regression exercises the same apply-response
	// contract as production: etcd/raft advances Applied from the last entry
	// in MsgStorageApplyResp. An index-only message would bypass that contract
	// and could leave the test green while the orchestrator path is broken.
	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         node.wal,
		Applied:         1,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Logger:          NewLoggerAdapter(logging.Testing()),
	})
	require.NoError(t, err)
	node.rawNode = rawNode

	runDone := make(chan error, 1)
	go func() { runDone <- setup.applier.Run(context.Background(), setup.stop) }()
	setup.applier.Submit(entries, nil, nil, setup.stop)
	setup.applier.Drain(setup.stop)
	close(setup.stop)
	require.NoError(t, <-runDone)
	require.Equal(t, uint64(4), node.fsm.LastPersistedIndex())

	// FSM=4, but the orchestrator has not stepped the apply response beyond 1.
	node.lastRaftApplied.Store(1)
	require.NoError(t, node.doMaintenance())
	snapshot, err := node.wal.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(4), snapshot.GetMetadata().GetIndex())
	_, err = node.wal.Entries(2, 5, ^uint64(0))
	require.NoError(t, err, "entries needed by the election scan must remain available")

	// Once the orchestrator receives the apply response for the whole batch,
	// the normal margin can apply. Production responses carry the applied
	// entries and leave Index unset; Step the real RawNode first, then record
	// the same response in the node's shadow cursor.
	applyResp := &raftpb.Message{
		Type:    raftpb.MsgStorageApplyResp.Enum(),
		To:      proto.Uint64(1),
		From:    proto.Uint64(raft.LocalApplyThread),
		Entries: entries,
	}
	require.NoError(t, node.rawNode.Step(applyResp))
	node.recordRaftApplyResponse(applyResp)
	require.Equal(t, uint64(4), node.rawNode.Status().Applied)
	require.Equal(t, uint64(4), node.lastRaftApplied.Load())
	require.NoError(t, node.doMaintenance())
	_, err = node.wal.Entries(2, 5, ^uint64(0))
	require.ErrorIs(t, err, raft.ErrCompacted)
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

// TestDoMaintenance_SnapshotSaveFailure_RetriesNextTick makes the snapshot file
// unwritable for a reason the next tick can retry: a file occupies the snapshot
// directory. The WAL must keep reporting the previous snapshot, so the next
// tick is not short-circuited by the no-new-index fast path, and the retry
// lands the snapshot once the directory is usable again.
func TestDoMaintenance_SnapshotSaveFailure_RetriesNextTick(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	node, setup := newTestMaintenanceNode(t)

	entry, _ := makeCreateLedgerEntry(t, 1, "test-ledger")
	setup.applyEntry(t, ctx, entry)

	snapDir := filepath.Join(setup.walDir, "snap")
	require.NoError(t, os.RemoveAll(snapDir))
	require.NoError(t, os.WriteFile(snapDir, nil, 0600))

	require.NoError(t, node.doMaintenance())

	snap, err := node.wal.Snapshot()
	require.NoError(t, err)
	require.Zero(t, snap.GetMetadata().GetIndex(), "a snapshot whose file was never written must not be published")

	require.NoError(t, os.Remove(snapDir))

	require.NoError(t, node.doMaintenance())

	snap, err = node.wal.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), snap.GetMetadata().GetIndex())
}
