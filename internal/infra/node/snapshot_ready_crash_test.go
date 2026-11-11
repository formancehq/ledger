package node

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// A received snapshot has a new HardState, but the old WAL must remain
// recoverable if the process stops before the replacement is installed.
func TestSnapshotReadyCrashBeforeWALReplacement(t *testing.T) {
	for _, divergentTail := range []bool{false, true} {
		name := "short old log"
		if divergentTail {
			name = "divergent old tail"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestSnapshotReadyCrashChild$")
			child.Env = append(os.Environ(), "LEDGER_SNAPSHOT_READY_CRASH_DIR="+dir)
			if divergentTail {
				child.Env = append(child.Env, "LEDGER_SNAPSHOT_READY_CRASH_TAIL=1")
			}
			output, err := child.CombinedOutput()
			require.NoError(t, err, string(output))

			reopened, err := wal.New(dir, logging.Testing(), noop.Meter{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			state, _, err := reopened.InitialState()
			require.NoError(t, err)
			require.Equal(t, uint64(3), state.GetTerm())
			require.Equal(t, uint64(2), state.GetVote())
			require.Equal(t, uint64(2), state.GetCommit())
			last, err := reopened.LastIndex()
			require.NoError(t, err)
			if divergentTail {
				require.Equal(t, uint64(5), last)
				term, err := reopened.Term(4)
				require.NoError(t, err)
				require.Equal(t, uint64(3), term)
			} else {
				require.Equal(t, uint64(2), last)
			}
			snapshot, err := reopened.Snapshot()
			require.NoError(t, err)
			require.Equal(t, uint64(2), snapshot.GetMetadata().GetIndex())
			_, err = raft.NewRawNode(&raft.Config{
				ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: reopened,
				Applied: 2, MaxInflightMsgs: 256, AsyncStorageWrites: true,
			})
			require.NoError(t, err, "the old WAL must still restart as Raft storage")
		})
	}
}

func TestSnapshotReadyCrashChild(t *testing.T) {
	dir := os.Getenv("LEDGER_SNAPSHOT_READY_CRASH_DIR")
	if dir == "" {
		return
	}
	w, err := wal.New(dir, logging.Testing(), noop.Meter{})
	require.NoError(t, err)
	entries := []*raftpb.Entry{
		{Index: proto.Uint64(1), Term: proto.Uint64(1)},
		{Index: proto.Uint64(2), Term: proto.Uint64(1)},
	}
	if os.Getenv("LEDGER_SNAPSHOT_READY_CRASH_TAIL") == "1" {
		for index := uint64(3); index <= 5; index++ {
			entries = append(entries, &raftpb.Entry{Index: new(index), Term: proto.Uint64(3)})
		}
	}
	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	require.NoError(t, w.Append(&raftpb.HardState{
		Term: proto.Uint64(3), Vote: proto.Uint64(2), Commit: proto.Uint64(2),
	}, entries))
	require.NoError(t, w.CreateSnapshot(2, cs, nil))
	rn, err := raft.NewRawNode(&raft.Config{
		ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: w,
		Applied: 2, MaxInflightMsgs: 256, AsyncStorageWrites: true,
	})
	require.NoError(t, err)
	snapshot := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
		Index: proto.Uint64(4), Term: proto.Uint64(2), ConfState: cs,
	}}
	require.NoError(t, rn.Step(&raftpb.Message{
		Type: new(raftpb.MsgSnap), From: proto.Uint64(2), To: proto.Uint64(1),
		Term: proto.Uint64(4), Snapshot: snapshot,
	}))
	require.True(t, rn.HasReady())
	ready := rn.Ready()
	require.True(t, proto.Equal(snapshot, ready.Snapshot), "Raft must accept the snapshot")
	require.Equal(t, uint64(4), ready.GetCommit())
	require.Empty(t, ready.Entries)
	n := &Node{wal: w}
	require.NoError(t, n.persistReady(context.Background(), ready))
	// Exit without closing the WAL or installing the snapshot. This is the
	// process-crash window before the first replacement-directory rename.
	os.Exit(0)
}
