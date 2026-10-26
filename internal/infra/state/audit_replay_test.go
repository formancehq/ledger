package state

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestAuditReplayerDisablesWAL(t *testing.T) {
	t.Parallel()

	replayer, err := NewAuditReplayer(logging.Testing(), "test-cluster")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, replayer.Close())
	})
	replayDir := replayer.store.DataDir()

	_, err = replayer.Replay(&commonpb.Timestamp{Data: 1}, []*raftcmdpb.Order{{
		Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
				CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
			},
		}},
	}})
	require.NoError(t, err)

	err = filepath.WalkDir(replayDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if info.Size() != 0 {
				return fmt.Errorf("unexpected WAL contents in %s", path)
			}
		}

		return nil
	})
	require.NoError(t, err)
}

func TestAuditReplayerUsesRecoveredQueryCheckpointCounter(t *testing.T) {
	t.Parallel()

	replayer, err := NewAuditReplayer(logging.Testing(), "test-cluster")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, replayer.Close()) })

	logs, err := replayer.Replay(&commonpb.Timestamp{Data: 1}, []*raftcmdpb.Order{{
		Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
				CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
			},
		}},
	}})
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(1), logs[0].GetPayload().GetCreatedQueryCheckpoint().GetCheckpointId())
}

// TestAuditReplayerCompilesScriptsItself: the audit keeps only the business
// part of an order, so a scripted order the checker re-runs never carries the
// compiled numscript code. The replay must compile the script itself — for an
// inline script and for one saved in the library — instead of failing with
// "no compiled numscript artifact".
func TestAuditReplayerCompilesScriptsItself(t *testing.T) {
	t.Parallel()

	const ledger = "test"

	ledgerOrder := func(payload func(*raftcmdpb.LedgerScopedOrder)) *raftcmdpb.Order {
		scoped := &raftcmdpb.LedgerScopedOrder{Ledger: ledger}
		payload(scoped)

		return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: scoped}}
	}
	createTx := func(tx *raftcmdpb.CreateTransactionOrder) *raftcmdpb.Order {
		return ledgerOrder(func(o *raftcmdpb.LedgerScopedOrder) {
			o.Payload = &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
				Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: tx},
			}}
		})
	}
	requirePosting := func(t *testing.T, logs []*commonpb.Log, amount uint64) {
		t.Helper()

		require.Len(t, logs, 1)
		postings := logs[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetPostings()
		require.Len(t, postings, 1)
		require.Equal(t, "world", postings[0].GetSource())
		require.Equal(t, "users:alice", postings[0].GetDestination())
		require.Equal(t, amount, postings[0].GetAmount().ToBigInt().Uint64())
	}

	replayer, err := NewAuditReplayer(logging.Testing(), "test-cluster")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, replayer.Close()) })

	at := &commonpb.Timestamp{Data: 1}

	_, err = replayer.Replay(at, []*raftcmdpb.Order{ledgerOrder(func(o *raftcmdpb.LedgerScopedOrder) {
		o.Payload = &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}}
	})})
	require.NoError(t, err)

	logs, err := replayer.Replay(at, []*raftcmdpb.Order{createTx(&raftcmdpb.CreateTransactionOrder{
		Script: &commonpb.Script{Plain: `send [USD/2 100] (
  source = @world
  destination = @users:alice
)`},
	})})
	require.NoError(t, err, "an inline script without compiled code must be compiled by the replay")
	requirePosting(t, logs, 100)

	_, err = replayer.Replay(at, []*raftcmdpb.Order{ledgerOrder(func(o *raftcmdpb.LedgerScopedOrder) {
		o.Payload = &raftcmdpb.LedgerScopedOrder_SaveNumscript{SaveNumscript: &raftcmdpb.SaveNumscriptOrder{
			Name:    "pay",
			Version: "1.0.0",
			Content: `vars {
  monetary $amt
}

send $amt (
  source = @world
  destination = @users:alice
)`,
		}}
	})})
	require.NoError(t, err)

	logs, err = replayer.Replay(at, []*raftcmdpb.Order{createTx(&raftcmdpb.CreateTransactionOrder{
		NumscriptReference: &raftcmdpb.NumscriptReference{
			Name:    "pay",
			Version: "latest",
			Vars:    map[string]string{"amt": "USD/2 250"},
		},
	})})
	require.NoError(t, err, "a library script without compiled code must be compiled by the replay")
	requirePosting(t, logs, 250)
}

func TestAuditReplayerLedgerBoundaries(t *testing.T) {
	t.Parallel()

	replayer, err := NewAuditReplayer(logging.Testing(), "test-cluster")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, replayer.Close()) })

	boundaries, err := replayer.LedgerBoundaries()
	require.NoError(t, err)
	require.Empty(t, boundaries)

	const ledger = "mirror-boundaries"
	create := createLedgerOrder(ledger)
	create.GetLedgerScoped().GetCreateLedger().Mode = commonpb.LedgerMode_LEDGER_MODE_MIRROR
	_, err = replayer.Replay(&commonpb.Timestamp{Data: 1}, []*raftcmdpb.Order{create})
	require.NoError(t, err)

	requireBoundaries := func(nextTx, nextLog, mirrorID uint64) {
		t.Helper()
		rows, err := replayer.LedgerBoundaries()
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Contains(t, rows, ledger, "export must decode the canonical ledger key")
		require.Equal(t, nextTx, rows[ledger].GetNextTransactionId())
		require.Equal(t, nextLog, rows[ledger].GetNextLogId())
		require.Equal(t, mirrorID, rows[ledger].GetLastMirrorV2LogId())
	}
	requireBoundaries(1, 1, 0)

	for _, step := range []struct {
		sourceID uint64
		skipped  uint64
		nextTx   uint64
		nextLog  uint64
		mirrorID uint64
		logCount int
	}{
		{sourceID: 1, skipped: 99, nextTx: 100, nextLog: 2, mirrorID: 1, logCount: 1},
		// A replayed source position is a no-op even if its serialized order
		// carries a larger skipped transaction ID.
		{sourceID: 1, skipped: 999, nextTx: 100, nextLog: 2, mirrorID: 1, logCount: 0},
		{sourceID: 2, skipped: 199, nextTx: 200, nextLog: 3, mirrorID: 2, logCount: 1},
	} {
		order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: ledger,
			Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{
				Entry: &raftcmdpb.MirrorLogEntry{
					V2LogId: step.sourceID,
					Data: &raftcmdpb.MirrorLogEntry_FillGap{FillGap: &raftcmdpb.MirrorFillGap{
						SkippedTransactionIds: []uint64{step.skipped},
					}},
				},
			}},
		}}}
		logs, err := replayer.Replay(&commonpb.Timestamp{Data: 2}, []*raftcmdpb.Order{order})
		require.NoError(t, err)
		require.Len(t, logs, step.logCount)
		requireBoundaries(step.nextTx, step.nextLog, step.mirrorID)
	}

	_, err = replayer.Replay(&commonpb.Timestamp{Data: 3}, []*raftcmdpb.Order{{
		Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: ledger,
			Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{
				DeleteLedger: &raftcmdpb.DeleteLedgerOrder{},
			},
		}},
	}})
	require.NoError(t, err)
	boundaries, err = replayer.LedgerBoundaries()
	require.NoError(t, err)
	require.Empty(t, boundaries, "canonical deletion removes the ledger boundary")
}
