package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestConfStateContainsNode(t *testing.T) {
	t.Parallel()

	t.Run("node in voters", func(t *testing.T) {
		t.Parallel()

		cs := raftpb.ConfState{Voters: []uint64{1, 2, 3}}
		require.True(t, confStateContainsNode(cs, 2))
	})

	t.Run("node in learners", func(t *testing.T) {
		t.Parallel()

		cs := raftpb.ConfState{
			Voters:   []uint64{1, 2},
			Learners: []uint64{3, 4},
		}
		require.True(t, confStateContainsNode(cs, 4))
	})

	t.Run("node absent", func(t *testing.T) {
		t.Parallel()

		cs := raftpb.ConfState{
			Voters:   []uint64{1, 2},
			Learners: []uint64{3},
		}
		require.False(t, confStateContainsNode(cs, 99))
	})

	t.Run("empty ConfState", func(t *testing.T) {
		t.Parallel()

		cs := raftpb.ConfState{}
		require.False(t, confStateContainsNode(cs, 1))
	})
}

// TestInitialJoinVoters_ExcludesSelf verifies the EN-1431 follow-up bug fix:
// when discoverPeersFromCluster echoes back the joining node's own ID (which
// happens when the leader's status.Progress still carries this node from a
// previous life — e.g. auto-promoted voter before a scale-down/scale-up),
// initialJoinVoters must drop self so the initial WAL snapshot does not end
// up with `Voters=[..., self], Learners=[self]`. That raft-invalid state
// triggers assertConfStatesEquivalent's panic on the next boot, producing a
// permanent CrashLoopBackOff that only wiping cluster-side state recovers
// from.
func TestInitialJoinVoters_ExcludesSelf(t *testing.T) {
	t.Parallel()

	t.Run("self absent from peers", func(t *testing.T) {
		t.Parallel()

		peers := []Peer{{ID: 1}, {ID: 3}}
		voters := initialJoinVoters(peers, 2)
		require.Equal(t, []uint64{1, 3}, voters)
	})

	t.Run("self present in peers is dropped", func(t *testing.T) {
		t.Parallel()

		peers := []Peer{{ID: 1}, {ID: 2}, {ID: 3}}
		voters := initialJoinVoters(peers, 2)
		require.Equal(t, []uint64{1, 3}, voters,
			"self must be excluded from the voter list to avoid the "+
				"Voters=[...,self]/Learners=[self] raft-invalid ConfState")
	})

	t.Run("self is the only peer", func(t *testing.T) {
		t.Parallel()

		peers := []Peer{{ID: 2}}
		voters := initialJoinVoters(peers, 2)
		require.Empty(t, voters)
	})

	t.Run("empty peers", func(t *testing.T) {
		t.Parallel()

		voters := initialJoinVoters(nil, 2)
		require.Empty(t, voters)
	})
}

func TestFinishReady_SnapshotInstall_PreservesWALConfState(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	node := &Node{
		logger:        logging.Testing(),
		wal:           setup.wal,
		fsm:           setup.fsm,
		applier:       setup.applier,
		peerAddresses: map[uint64]ConfChangeContext{},
	}

	// Build a real rawNode backed by the WAL (raft.Storage), before installing
	// the snapshot, so ReportSnapshot in finishReady does not nil-panic.
	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         setup.wal,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Logger:          NewLoggerAdapter(logging.Testing()),
	})
	require.NoError(t, err)
	node.rawNode = rawNode

	// Stale in-memory shadow: the node's pre-snapshot membership view.
	node.confState.Store(&raftpb.ConfState{Voters: []uint64{1, 2}})

	// Install a snapshot carrying a membership delta (node 3 added) into the
	// real WAL, exactly as processReady does via wal.ApplySnapshot.
	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     10,
			Term:      2,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}
	require.NoError(t, node.wal.ApplySnapshot(snap))

	// Sanity: the WAL now holds the correct ConfState.
	_, csBefore, err := node.wal.InitialState()
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3}, csBefore.Voters)

	// Drive finishReady for the snapshot Ready.
	stop := make(chan struct{})
	result := readyResult{
		rd:              raft.Ready{Snapshot: snap},
		snapshotApplied: true,
	}
	require.NoError(t, node.finishReady(result, stop))

	// The reconcile must NOT have overwritten the WAL with the stale shadow.
	_, csAfter, err := node.wal.InitialState()
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3}, csAfter.Voters,
		"WAL ConfState must match the installed snapshot, not the stale shadow")

	// The in-memory shadow must also be refreshed for downstream readers.
	require.Equal(t, []uint64{1, 2, 3}, node.confState.Load().Voters,
		"in-memory confState shadow must be refreshed from the snapshot")
}
