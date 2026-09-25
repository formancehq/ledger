package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/application/usagebuilder"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

func TestStatsAllowsUsageLagAndConverges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := logging.NopZap()
	meter := noop.NewMeterProvider().Meter("test")
	primary, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, primary.Close()) })
	usage, err := usagestore.New(t.TempDir(), logger, usagestore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, usage.Close()) })
	attrs := attributes.New()
	const ledger = "default"
	batch := primary.OpenWriteSession()
	require.NoError(t, batch.SetProto(dal.NewKeyBuilder().PutZonePrefix(dal.ZoneGlobal, dal.SubGlobLedgerInfo).PutLedgerName(ledger).Build(), &commonpb.LedgerInfo{Name: ledger}))
	_, err = attrs.Boundary.Set(batch, domain.LedgerKey{Name: ledger}.Bytes(), &raftcmdpb.LedgerBoundaries{NextTransactionId: 2, NextLogId: 2})
	require.NoError(t, err)
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: ledger, Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
			Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: &raftcmdpb.CreateTransactionOrder{}},
		}},
	}}}
	raw, err := order.MarshalVT()
	require.NoError(t, err)
	key := func(sub byte) *dal.KeyBuilder {
		return dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, sub).PutUint64(1)
	}
	require.NoError(t, batch.SetProto(key(dal.SubHistoryAudit).Build(), &auditpb.AuditEntry{Sequence: 1, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}}}))
	require.NoError(t, batch.SetProto(key(dal.SubHistoryAppliedProposal).Build(), &proposalpb.AppliedProposal{Sequence: 1}))
	require.NoError(t, batch.SetProto(key(dal.SubHistoryAuditItem).PutUint32(0).Build(), &auditpb.AuditItem{SerializedOrder: raw, LogSequence: 1}))
	require.NoError(t, batch.SetProto(key(dal.SubHistoryLog).Build(), &commonpb.Log{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: ledger, Log: &commonpb.LedgerLog{Id: 1, Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{
			Transaction: &commonpb.Transaction{Id: 1, Postings: []*commonpb.Posting{{Source: "world", Destination: "user", Asset: "USD", Amount: commonpb.NewUint256FromUint64(1)}}},
		}}}},
	}}}}))
	require.NoError(t, batch.Commit())
	controller := ctrl.NewDefaultController(nil, primary, logger, attrs, nil, usage, meter)

	// The builder has not started: this is a deterministic legal projection lag,
	// not a fabricated response or a timer race.
	stats, err := controller.GetLedgerStats(ctx, ledger)
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.GetTransactionCount())
	require.Equal(t, uint64(1), stats.GetLogCount())
	require.Zero(t, stats.GetPostingCount())
	require.True(t, mainStoreStatsConsistent(stats), "legal asynchronous usage lag must not fail the live stats oracle: tx=%d posting=%d", stats.GetTransactionCount(), stats.GetPostingCount())

	// A separate progress witness, not the posting value being tested, qualifies
	// the final observation. Exercise the real audit-to-usage builder.
	builder := usagebuilder.NewBuilder(primary, usage, nil, logger, meter, 1)
	builder.Start()
	t.Cleanup(builder.Stop)
	require.Eventually(t, func() bool { return builder.LastProcessedAuditSequence() == 1 }, 5*time.Second, time.Millisecond)
	progress, err := usage.ReadProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(1), progress)
	stats, err = controller.GetLedgerStats(ctx, ledger)
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.GetPostingCount())
	require.Zero(t, stats.GetRevertCount())
}

func TestLiveStatsPreservesMainInvariantWithoutMixingUsage(t *testing.T) {
	t.Parallel()
	require.False(t, mainStoreStatsConsistent(&commonpb.LedgerStats{TransactionCount: 2, LogCount: 1}))
	require.True(t, mainStoreStatsConsistent(&commonpb.LedgerStats{TransactionCount: 1, LogCount: 1, PostingCount: 4, RevertCount: 2}),
		"usage may be ahead of the earlier main-store snapshot")
}
