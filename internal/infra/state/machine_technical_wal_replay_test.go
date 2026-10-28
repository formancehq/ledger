package state

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// Startup replay reads only committed entries from a reopened disk WAL, then
// synchronously applies those original command bytes. Persisted marker values
// must select the same interpretation as live apply, including historical
// rejection effects. The final uncommitted entry proves the commit boundary.
func TestTechnicalAtomicity_DiskWALReplayPreservesCommandSemantics(t *testing.T) {
	t.Parallel()
	for _, atomic := range []bool{false, true} {
		t.Run(map[bool]string{false: "historical", true: "atomic"}[atomic], func(t *testing.T) {
			t.Parallel()
			previous := makeProposal(1)
			previous.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{
				sinkCursorUpdate("preserved", 7), sinkCursorUpdate("rejected", 11),
			}
			rejected := technicalCoverageRejection(2, sinkCursorUpdate("rejected", 999))
			rejected.AtomicTechnicalUpdates = atomic
			uncommitted := makeProposal(3)
			uncommitted.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{sinkCursorUpdate("preserved", 12345)}
			entries := []*raftpb.Entry{
				makeEntry(t, 1, previous), unsealedTechnicalEntry(t, 2, rejected), makeEntry(t, 3, uncommitted),
			}

			logger := logging.FromContext(logging.TestingContext())
			meter := noop.NewMeterProvider().Meter("test")
			directory := t.TempDir()
			writer, err := wal.New(directory, logger, meter)
			require.NoError(t, err)
			require.NoError(t, writer.Append(&raftpb.HardState{Term: new(uint64(1)), Vote: new(uint64(1)), Commit: new(uint64(2))}, entries))
			require.NoError(t, writer.Close())

			reopened, err := wal.New(directory, logger, meter)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			hardState, _, err := reopened.InitialState()
			require.NoError(t, err)
			require.Equal(t, uint64(2), hardState.GetCommit())
			last, err := reopened.LastIndex()
			require.NoError(t, err)
			require.Equal(t, uint64(3), last, "the disk WAL also contains an uncommitted tail")
			replayed, err := reopened.Entries(1, hardState.GetCommit()+1, math.MaxUint64)
			require.NoError(t, err)
			require.Len(t, replayed, 2)
			for i, entry := range replayed {
				require.Equal(t, entries[i].GetData(), entry.GetData(), "disk recovery must retain exact command bytes")
			}
			var recoveredCommand raftcmdpb.Proposal
			require.NoError(t, recoveredCommand.UnmarshalVT(replayed[1].GetData()))
			require.Equal(t, atomic, recoveredCommand.GetAtomicTechnicalUpdates())

			// Apply the same serialized committed entries both directly and from
			// the reopened WAL; compare observable durable and recovered state.
			for _, run := range []struct {
				name    string
				entries []*raftpb.Entry
			}{{name: "live", entries: entries[:2]}, {name: "disk replay", entries: replayed}} {
				t.Run(run.name, func(t *testing.T) {
					machine, store, _ := newTestMachine(t)
					applied, err := machine.ApplyEntries(t.Context(), store, run.entries...)
					require.NoError(t, err)
					require.Len(t, applied.Results, 2)
					require.NoError(t, applied.Results[0].Error)
					requireTechnicalCoverageRejection(t, applied.Results[1].Error)
					require.False(t, applied.Results[1].AuditEntryWritten)
					want := uint64(999)
					if atomic {
						want = 11
					}
					requireTechnicalCursor(t, store, "rejected", want)
					requireTechnicalCursor(t, store, "preserved", 7)
					require.Equal(t, uint64(2), machine.LastAppliedIndex())
					require.Equal(t, uint64(1), machine.State.NextAuditSequenceID)
					recovered := recoverMachineOnStore(t, store)
					require.NoError(t, NewRecovery(recovered, store).RestoreCacheFromStore())
					require.Equal(t, uint64(2), recovered.LastAppliedIndex())
					require.Equal(t, uint64(1), recovered.State.NextAuditSequenceID)
					requireTechnicalCursor(t, store, "rejected", want)
					requireTechnicalCursor(t, store, "preserved", 7)
				})
			}
		})
	}
}
