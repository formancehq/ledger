package ctrl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestAnalyzeTransactions_CancellationReleasesSnapshotLock(t *testing.T) {
	t.Parallel()

	const ledger = "cancel-analysis"
	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")
	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	logs := make([]*commonpb.Log, 501)
	for i := range logs {
		logs[i] = &commonpb.Log{
			Sequence: uint64(i + 1),
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
				LedgerName: ledger,
				Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
						CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{}},
					},
				}},
			}}},
		}
	}
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger}))
	require.NoError(t, state.AppendLogs(batch, logs))
	require.NoError(t, batch.Commit())

	ctrl := NewDefaultController(nil, store, logger, attributes.New(), nil, nil, meter)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	progressCalls := 0
	_, err = ctrl.AnalyzeTransactions(ctx, ledger, 0, func(processed, _ uint64) {
		progressCalls++
		if processed == 500 {
			cancel()
		}
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, progressCalls)

	closed := make(chan error, 1)
	go func() { closed <- store.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("store close blocked after canceled analysis; snapshot read lock was retained")
	}
}
