package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/raftutil"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// TestRaftApplied_ControlsCommittedEntriesDelivery empirically demonstrates
// etcd raft's contract around Config.Applied:
//
//   - With Config.Applied = N (current OLD code's behaviour when WAL snapshot
//     is at N), Raft returns NO committed entries to the application, even
//     when committed entries exist between the FSM's actual durable applied
//     index and N. Those entries are silently dropped from the application's
//     view.
//
//   - With Config.Applied = M (the FSM's actual durable applied index from
//     Pebble), Raft returns entries [M+1, Commit] in CommittedEntries, which
//     the application can then re-apply. This is the standard Raft replay
//     mechanism.
//
// This proves that the "applied = walSnap.Metadata.Index" line in Node.Run
// causes silent data loss when Pebble's durable state lags the WAL snapshot
// (the post-power-loss scenario). There is no other mechanism in etcd raft
// or in this codebase that would re-deliver the [M+1, N] entries.
func TestRaftApplied_ControlsCommittedEntriesDelivery(t *testing.T) {
	t.Parallel()

	walDir := t.TempDir()
	logger := logging.Testing()
	meter := noop.Meter{}

	w, err := wal.New(walDir, logger, meter)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	confState := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(0, confState, nil))

	// Append 5 raft entries with HardState.Commit = 5. This simulates the
	// state after the cluster has committed and (memtable-only) applied 5
	// entries.
	entries := make([]*raftpb.Entry, 5)
	for i := range entries {
		entries[i] = &raftpb.Entry{
			Term:  proto.Uint64(1),
			Index: new(uint64(i + 1)),
			Type:  raftutil.EntryType(raftpb.EntryNormal),
			Data:  []byte{byte(i)},
		}
	}

	require.NoError(t, w.Append(&raftpb.HardState{Term: proto.Uint64(1), Vote: proto.Uint64(1), Commit: proto.Uint64(5)}, entries))

	makeRaftNode := func(t *testing.T, applied uint64) *raft.RawNode {
		t.Helper()

		cfg := &raft.Config{
			ID:              1,
			ElectionTick:    10,
			HeartbeatTick:   1,
			Storage:         w,
			Applied:         applied,
			MaxSizePerMsg:   1024 * 1024,
			MaxInflightMsgs: 256,
			Logger:          NewLoggerAdapter(logger),
		}
		rn, err := raft.NewRawNode(cfg)
		require.NoError(t, err)

		return rn
	}

	t.Run("Applied=5 (OLD code path): Raft delivers NO committed entries", func(t *testing.T) {
		t.Parallel()

		rn := makeRaftNode(t, 5)

		var delivered []*raftpb.Entry

		if rn.HasReady() {
			rd := rn.Ready()
			delivered = append(delivered, rd.CommittedEntries...)
			rn.Advance(rd)
		}

		assert.Empty(t, delivered,
			"with Applied=5, Raft considers [1, 5] already applied — entries are never re-delivered, "+
				"FSM never re-executes them, side-effects are LOST")
	})

	t.Run("Applied=3 (FSM's real durable index): Raft delivers [4, 5]", func(t *testing.T) {
		t.Parallel()

		rn := makeRaftNode(t, 3)

		require.True(t, rn.HasReady(),
			"with Applied=3 and Commit=5, Raft must have entries [4, 5] to deliver")

		rd := rn.Ready()
		require.Len(t, rd.CommittedEntries, 2,
			"Raft must deliver entries [Applied+1, Commit] = [4, 5]")

		assert.Equal(t, uint64(4), rd.CommittedEntries[0].GetIndex())
		assert.Equal(t, uint64(5), rd.CommittedEntries[1].GetIndex())
	})
}
