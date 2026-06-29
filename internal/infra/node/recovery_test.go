package node

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// TestRun_RefusesStartWhenGapExceedsWALRetention verifies the only hard
// failure mode introduced by the durability fix: when Pebble's durable
// applied index is behind the Raft WAL's first available entry, the entries
// needed to catch the FSM up via Raft replay are gone from both Pebble and
// the WAL. Recovery would silently lose them; the node refuses to start.
//
// Reaching this state requires the durability gap between
// fsm.lastPersistedIndex and Pebble's durable applied index to exceed the
// compaction margin between two maintenance ticks — pathological but
// possible under sustained write bursts with a small margin.
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
		logger:     logging.Testing(),
		wal:        setup.wal,
		fsm:        setup.fsm,
		applier:    setup.applier,
		membership: newTestMembership(t),
	}
	node.confState.Store(setup.confState)

	ready := make(chan struct{})
	err := node.Run(context.Background(), ready)

	require.Error(t, err)
	require.Contains(t, err.Error(), "durability gap exceeds WAL retention")
	require.Contains(t, err.Error(), "Pebble applied=0")
}
