package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestFindCheckpointBoundary covers the applier-side pre-split that keeps a
// checkpoint trigger at the END of the FSM batch. The boundary returned is
// the length of the prefix that should be applied in one call to
// applyEntriesPipelined.
func TestFindCheckpointBoundary(t *testing.T) {
	t.Parallel()

	apply := func(t *testing.T, index uint64) raftpb.Entry {
		t.Helper()
		cmd := commands.NewCommand(&raftcmdpb.Order{
			Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "ledger",
					Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
						CreateLedger: &raftcmdpb.CreateLedgerOrder{},
					},
				},
			},
		})
		data, err := cmd.MarshalVT()
		require.NoError(t, err)

		return raftpb.Entry{Index: index, Term: 1, Type: raftpb.EntryNormal, Data: data}
	}

	checkpoint := func(t *testing.T, index uint64) raftpb.Entry {
		t.Helper()
		cmd := commands.NewCommand(&raftcmdpb.Order{
			Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{
					Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
						CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
					},
				},
			},
		})
		data, err := cmd.MarshalVT()
		require.NoError(t, err)

		return raftpb.Entry{Index: index, Term: 1, Type: raftpb.EntryNormal, Data: data}
	}

	emptyEntry := raftpb.Entry{Index: 42, Term: 1, Type: raftpb.EntryNormal}

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()
		boundary, err := findCheckpointBoundary(nil)
		require.NoError(t, err)
		require.Equal(t, 0, boundary)
	})

	t.Run("no trigger", func(t *testing.T) {
		t.Parallel()
		entries := []raftpb.Entry{apply(t, 1), apply(t, 2), apply(t, 3)}
		boundary, err := findCheckpointBoundary(entries)
		require.NoError(t, err)
		require.Equal(t, len(entries), boundary)
	})

	t.Run("trigger first", func(t *testing.T) {
		t.Parallel()
		entries := []raftpb.Entry{checkpoint(t, 1), apply(t, 2)}
		boundary, err := findCheckpointBoundary(entries)
		require.NoError(t, err)
		require.Equal(t, 1, boundary, "boundary includes the trigger; tail goes to the spool")
	})

	t.Run("trigger middle", func(t *testing.T) {
		t.Parallel()
		entries := []raftpb.Entry{apply(t, 1), apply(t, 2), checkpoint(t, 3), apply(t, 4), apply(t, 5)}
		boundary, err := findCheckpointBoundary(entries)
		require.NoError(t, err)
		require.Equal(t, 3, boundary)
	})

	t.Run("trigger last", func(t *testing.T) {
		t.Parallel()
		entries := []raftpb.Entry{apply(t, 1), apply(t, 2), checkpoint(t, 3)}
		boundary, err := findCheckpointBoundary(entries)
		require.NoError(t, err)
		require.Equal(t, len(entries), boundary)
	})

	t.Run("empty data entry is skipped", func(t *testing.T) {
		t.Parallel()
		entries := []raftpb.Entry{emptyEntry, apply(t, 43), checkpoint(t, 44)}
		boundary, err := findCheckpointBoundary(entries)
		require.NoError(t, err)
		require.Equal(t, len(entries), boundary)
	})
}
