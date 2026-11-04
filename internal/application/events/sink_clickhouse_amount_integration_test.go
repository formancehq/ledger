//go:build clickhouse

package events_test

import (
	"context"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/application/events"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// Count actual driver outcomes, without substituting a mocked acknowledgement.
type countedClickHouseSink struct {
	*events.ClickHouseSink

	failures  atomic.Int64
	successes atomic.Int64
}

func (s *countedClickHouseSink) Publish(ctx context.Context, batch []*eventspb.Event) error {
	err := s.ClickHouseSink.Publish(ctx, batch)
	if err != nil {
		s.failures.Add(1)
	} else {
		s.successes.Add(1)
	}

	return err
}

func TestClickHouseSinkIntegration_ExactAmountsAndCursorRecovery(t *testing.T) {
	t.Parallel()
	table := uniqueTopic("ledger_amounts")
	conn := openTestClickHouseConn(t, sharedClickHouseDSN)
	sink, err := events.NewClickHouseSink(t.Context(), events.ClickHouseSinkConfig{DSN: sharedClickHouseDSN, Table: table})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	counted := &countedClickHouseSink{ClickHouseSink: sink}

	store := newTestStore(t)
	registerLedger(t, store, "orders")
	now := libtime.Now()
	amounts := []string{
		"9007199254740993", "18446744073709551615", "18446744073709551617",
		"115792089237316195423570985008687907853269984665640564039457584007913129639935",
	}
	var logs []*commonpb.Log
	for i, amount := range amounts {
		value, ok := new(big.Int).SetString(amount, 10)
		require.True(t, ok)
		for _, reverted := range []bool{false, true} {
			tx := commonpb.NewTransaction().WithID(uint64(len(logs) + 1)).WithTimestamp(now).
				WithPostings(commonpb.NewPosting("world", "bank", "USD", value))
			data := &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
				CreatedTransaction: &commonpb.CreatedTransaction{Transaction: tx},
			}}
			if reverted {
				data.Payload = &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{
					RevertedTransactionId: uint64(i*2 + 1), RevertTransaction: tx,
				}}
			}
			logs = append(logs, &commonpb.Log{Sequence: uint64(len(logs) + 1), Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "orders", Log: commonpb.NewLedgerLog(data).WithID(uint64(len(logs) + 1)).WithDate(now),
				}},
			}})
		}
	}
	appendTestLogs(t, store, logs...)

	// Force a real Send rejection and observe the applied error before retrying.
	// Removing the constraint repairs the external destination without changing
	// logs, sink identity or cursor. This is same-process resume, not crash proof.
	require.NoError(t, conn.Exec(t.Context(), "ALTER TABLE "+table+" ADD CONSTRAINT delivery_blocked CHECK log_sequence = 0"))
	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 1
	newEmitter := func() *events.Emitter {
		return events.NewEmitter(store, counted, "amounts", &directProposer{store: store}, newPlanBuilder(t, store), logging.Testing(), cfg)
	}
	status := func() *commonpb.SinkStatus {
		handle, err := store.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { require.NoError(t, handle.Close()) }()
		statuses, err := query.BuildSinkStatuses(handle, []*commonpb.SinkConfig{{Name: "amounts"}})
		require.NoError(t, err)
		require.Len(t, statuses, 1)

		return statuses[0]
	}
	emitter := newEmitter()
	emitter.Start()
	t.Cleanup(emitter.Stop)
	require.Eventually(t, func() bool { return status().GetError() != nil }, 10*time.Second, 10*time.Millisecond)
	emitter.Stop()
	failed := status()
	require.Zero(t, failed.GetCursor())
	require.Contains(t, failed.GetError().GetMessage(), "delivery_blocked")
	require.Positive(t, counted.failures.Load())
	require.Zero(t, counted.successes.Load())
	failures := counted.failures.Load()
	var rowCount uint64
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT count() FROM "+table+" FINAL").Scan(&rowCount))
	require.Zero(t, rowCount)

	require.NoError(t, conn.Exec(t.Context(), "ALTER TABLE "+table+" DROP CONSTRAINT delivery_blocked"))
	resumed := newEmitter()
	resumed.Start()
	t.Cleanup(resumed.Stop)
	require.Eventually(t, func() bool { return status().GetCursor() == uint64(len(logs)) }, 10*time.Second, 10*time.Millisecond)
	resumed.Stop()
	require.Nil(t, status().GetError(), "successful publication must clear the applied failure")
	require.Equal(t, failures, counted.failures.Load(), "the repaired destination must accept every retry")
	require.Equal(t, int64(len(logs)), counted.successes.Load(), "batch size one must acknowledge each pending event once")
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT count() FROM "+table+" FINAL").Scan(&rowCount))
	require.Equal(t, uint64(len(logs)), rowCount)
	for i, expected := range amounts {
		for j, key := range []string{"transaction", "revertTransaction"} {
			var amount, amountType string
			require.NoError(t, conn.QueryRow(t.Context(), "SELECT toString(data."+key+".postings.amount[1]), toTypeName(data."+key+".postings.amount) FROM "+table+" FINAL WHERE log_sequence = ?", uint64(i*2+j+1)).Scan(&amount, &amountType))
			require.Equal(t, expected, amount)
			require.Equal(t, "Array(UInt256)", amountType)
		}
	}
}
