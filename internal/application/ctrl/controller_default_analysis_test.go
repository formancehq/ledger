package ctrl

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func newAnalysisController(t *testing.T) (*DefaultController, *dal.Store, *attributes.Attributes) {
	t.Helper()
	store := newCtrlTestStore(t)
	attrs := attributes.New()
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, "analysis", &commonpb.LedgerInfo{Name: "analysis"}))
	require.NoError(t, batch.Commit())

	return NewDefaultController(nil, store, logging.NopZap(), attrs, nil, nil, noop.NewMeterProvider().Meter("test")), store, attrs
}

func TestAnalyzeAccountsStopsOnCancellation(t *testing.T) {
	t.Parallel()
	ctrl, store, attrs := newAnalysisController(t)
	batch := store.OpenWriteSession()
	for i := range 1001 {
		key := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "analysis", Account: fmt.Sprintf("users:%04d", i)}, Key: "name"}
		_, err := attrs.Metadata.Set(batch, key.Bytes(), commonpb.NewStringValue("user"))
		require.NoError(t, err)
	}
	require.NoError(t, batch.Commit())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var callbacks int
	result, err := ctrl.AnalyzeAccounts(ctx, "analysis", 0, func(processed, total uint64) {
		callbacks++
		require.Equal(t, uint64(500), processed)
		require.Zero(t, total)
		cancel()
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
	require.Equal(t, 1, callbacks, "cancellation must stop before processing the next progress interval")
	require.NoError(t, store.Close(), "analysis must release its iterators and snapshot before returning")
}

func TestAnalyzeTransactionsStopsOnCancellation(t *testing.T) {
	t.Parallel()
	for _, pass := range []string{"first", "second"} {
		t.Run(pass, func(t *testing.T) {
			t.Parallel()
			ctrl, store, _ := newAnalysisController(t)
			batch := store.OpenWriteSession()
			logs := make([]*commonpb.Log, 1001)
			for i := range logs {
				logs[i] = &commonpb.Log{
					Sequence: uint64(i + 1),
					Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "analysis",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
							CreatedTransaction: &commonpb.CreatedTransaction{Transaction: commonpb.NewTransaction()},
						}}),
					}}},
				}
			}
			require.NoError(t, state.AppendLogs(batch, logs))
			require.NoError(t, batch.Commit())
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var processedAtCancel uint64
			var callbacksAfterCancel int
			result, err := ctrl.AnalyzeTransactions(ctx, "analysis", 0, func(processed, total uint64) {
				if processedAtCancel != 0 {
					callbacksAfterCancel++
				}
				if processedAtCancel == 0 && ((pass == "first" && total == 0) || (pass == "second" && total != 0)) {
					processedAtCancel = processed
					cancel()
				}
			})
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, result)
			if pass == "first" {
				require.Equal(t, uint64(500), processedAtCancel)
				require.ErrorContains(t, err, "pass 1")
			} else {
				require.Equal(t, uint64(1501), processedAtCancel)
				require.ErrorContains(t, err, "pass 2")
			}
			require.Zero(t, callbacksAfterCancel, "no further progress after cancellation")
			require.NoError(t, store.Close(), "analysis must release both scan cursors and snapshot before returning")
		})
	}
}

// cancelAfterChecksContext gives a scan a deterministic cancellation boundary
// even when every scanned record is filtered and no progress callback can run.
type cancelAfterChecksContext struct {
	context.Context

	cancel    context.CancelFunc
	remaining int
}

func (ctx *cancelAfterChecksContext) Err() error {
	ctx.remaining--
	if ctx.remaining == 0 {
		ctx.cancel()
	}

	return ctx.Context.Err()
}

func TestAnalyzeTransactionsCancellationWhileFilteringLogs(t *testing.T) {
	t.Parallel()
	ctrl, store, _ := newAnalysisController(t)
	batch := store.OpenWriteSession()
	logs := make([]*commonpb.Log, 10)
	for i := range logs {
		logs[i] = &commonpb.Log{Sequence: uint64(i + 1), Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{LedgerName: "other-ledger"},
		}}}
	}
	require.NoError(t, state.AppendLogs(batch, logs))
	require.NoError(t, batch.Commit())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	scanCtx := &cancelAfterChecksContext{Context: ctx, cancel: cancel, remaining: 3}
	result, err := ctrl.AnalyzeTransactions(scanCtx, "analysis", 0, func(uint64, uint64) {
		t.Fatal("filtered logs must never produce transaction progress")
	})
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "pass 1")
	require.Nil(t, result)
	require.NoError(t, store.Close(), "the filtered scan must release its cursor and snapshot")
}
