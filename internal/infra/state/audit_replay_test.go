package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

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
