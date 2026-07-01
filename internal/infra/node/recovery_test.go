package node

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// TestRun_RefusesStartWhenGapExceedsWALRetention verifies that when the applier
// is NOT out-of-sync but Pebble's applied lags the WAL's first available entry,
// node.Run refuses to start. This is the "genuine data-loss" branch of the
// durability guard: the maintenance invariant (SyncWAL before Compact) has been
// violated and the missing entries exist nowhere reachable.
func TestRun_RefusesStartWhenGapExceedsWALRetention(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	// Build a WAL state where snapshot is at 5 and entries [1, 2] have been
	// compacted away, while Pebble's lastAppliedIndex remains at 0.
	entries := make([]raftpb.Entry, 5)
	for i := range entries {
		entries[i] = raftpb.Entry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  raftpb.EntryNormal,
			Data:  []byte{byte(i)},
		}
	}

	require.NoError(t, setup.wal.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 5}, entries))
	require.NoError(t, setup.wal.CreateSnapshot(5, setup.confState, nil))
	require.NoError(t, setup.wal.Compact(3))

	node := &Node{
		logger:        logging.Testing(),
		wal:           setup.wal,
		fsm:           setup.fsm,
		applier:       setup.applier,
		peerAddresses: map[uint64]ConfChangeContext{},
	}
	node.confState.Store(setup.confState)

	ready := make(chan struct{})
	err := node.Run(context.Background(), ready)

	require.Error(t, err)
	require.Contains(t, err.Error(), "durability gap exceeds WAL retention")
	require.Contains(t, err.Error(), "Pebble applied=0")
}

// TestRun_AllowsOutOfSyncRecoveryPastGap verifies EN-1431: when the same
// applied<walFirstIdx condition holds AND the applier is already flagged
// out-of-sync (RecoverAndReplay observed LastAppliedIndex < SnapshotIndex),
// node.Run must NOT refuse to start with the durability-gap error. This is
// the recoverable "crashed post-InstallSnapshot, pre-SynchronizeWithLeader"
// state: WAL was compacted to snapshotIndex by etcd-raft synchronously, but
// the async SynchronizeWithLeader did not populate Pebble before the crash.
// The applier will re-trigger SyncSnapshot from processReady once a leader is
// detected.
//
// With the minimal test setup (no NodeID, no transport, no full raft config),
// Run either returns an error or panics further down in raft.newRaft — the
// assertion is that whichever way it fails, the failure must NOT carry the
// durability-gap message.
func TestRun_AllowsOutOfSyncRecoveryPastGap(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	entries := make([]raftpb.Entry, 5)
	for i := range entries {
		entries[i] = raftpb.Entry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  raftpb.EntryNormal,
			Data:  []byte{byte(i)},
		}
	}

	require.NoError(t, setup.wal.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 5}, entries))
	require.NoError(t, setup.wal.CreateSnapshot(5, setup.confState, nil))
	require.NoError(t, setup.wal.Compact(3))

	// Flag the applier as out-of-sync, matching what RecoverAndReplay would
	// do in production when it observes LastAppliedIndex < SnapshotIndex.
	setup.applier.setOutOfSync()

	node := &Node{
		logger:        logging.Testing(),
		wal:           setup.wal,
		fsm:           setup.fsm,
		applier:       setup.applier,
		peerAddresses: map[uint64]ConfChangeContext{},
	}
	node.confState.Store(setup.confState)

	var runErr error

	func() {
		defer func() {
			if r := recover(); r != nil {
				// Any panic that reaches here came from further down in Run
				// (etcd-raft in the minimal test setup). It must NOT be the
				// durability-gap panic, and it PROVES the check at node.go:789
				// was bypassed.
				require.NotContains(t, fmt.Sprint(r), "durability gap exceeds WAL retention")
			}
		}()

		ready := make(chan struct{})
		runErr = node.Run(context.Background(), ready)
	}()

	if runErr != nil {
		require.NotContains(t, runErr.Error(), "durability gap exceeds WAL retention")
	}
}
