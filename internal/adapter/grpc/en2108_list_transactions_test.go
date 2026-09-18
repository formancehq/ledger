package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Exercise the real ListTransactions handler/controller/cursor while its first
// stream is still sending from the checkpoint. Only the stream transport is
// mocked, so the second request must acquire the same frozen Pebble stores.
func TestEN2108OverlappingCheckpointListTransactions(t *testing.T) {
	t.Parallel()

	const ledger = "checkpoint-list-transactions"
	attrs := attributes.New()
	impl := newCheckpointGateFixture(t, func(store *dal.Store) {
		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger}))
		// The thirteenth row exercises the peek that produces a next-page
		// cursor while the request itself returns exactly twelve rows.
		for id := uint64(1); id <= 13; id++ {
			key := domain.TransactionKey{LedgerName: ledger, ID: id}
			_, err := attrs.Transaction.Set(batch, key.Bytes(), &commonpb.TransactionState{CreatedByLog: id})
			require.NoError(t, err)
			require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{
				Sequence: id,
				Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{
							Transaction: &commonpb.Transaction{Id: id},
						}},
					}},
				}}},
			}}))
		}
		require.NoError(t, batch.Commit())
	})
	impl.localCtrl = ctrl.NewDefaultController(nil, impl.store, impl.logger, attrs, impl.readStore, nil, noop.NewMeterProvider().Meter("test"))
	req := &servicepb.ListTransactionsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: 12,
			Reverse:  true,
			Read:     &commonpb.ReadOptions{CheckpointId: gateCheckpointID},
		},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	mocks := gomock.NewController(t)
	sending, unblock := make(chan struct{}), make(chan struct{})
	firstDone := make(chan error, 1)
	var firstRows, secondRows []uint64
	var firstTrailer, secondTrailer metadata.MD
	newStream := func(rows *[]uint64, trailer *metadata.MD, hold bool) *MockServerStreamingServer[commonpb.Transaction] {
		stream := NewMockServerStreamingServer[commonpb.Transaction](mocks)
		stream.EXPECT().Context().Return(ctx).AnyTimes()
		stream.EXPECT().Send(gomock.Any()).DoAndReturn(func(tx *commonpb.Transaction) error {
			*rows = append(*rows, tx.GetId())
			if hold && len(*rows) == 1 {
				close(sending)
				select {
				case <-unblock:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		}).AnyTimes()
		stream.EXPECT().SetTrailer(gomock.Any()).Do(func(md metadata.MD) {
			*trailer = metadata.Join(*trailer, md)
		}).AnyTimes()

		return stream
	}
	first := newStream(&firstRows, &firstTrailer, true)
	second := newStream(&secondRows, &secondTrailer, false)
	// Always let the first handler finish and close its cursor/stores before
	// fixture cleanup, including when the second open fails on the old code.
	var firstResult *error
	defer func() {
		close(unblock)
		if firstResult == nil {
			select {
			case err := <-firstDone:
				firstResult = &err
			case <-time.After(5 * time.Second):
				t.Error("first ListTransactions handler did not finish")

				return
			}
		}
		require.NoError(t, *firstResult)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, firstRows)
		require.Equal(t, []string{"12"}, firstTrailer.Get(NextCursorTrailerKey))
	}()
	go func() { firstDone <- impl.ListTransactions(req, first) }()
	select {
	case <-sending:
	case err := <-firstDone:
		firstResult = &err
		t.Fatalf("first handler ended before sending a row: %v", err)
	case <-ctx.Done():
		t.Fatal("first handler did not reach the sending barrier")
	}
	err := impl.ListTransactions(req, second)
	if err != nil {
		t.Logf("raw=%v; gRPC=%s", err, status.Code(convertToGRPCError(err, testLogger())))
	}
	require.NoError(t, err, "a second ListTransactions stream must succeed while the first still holds its checkpoint stores")
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, secondRows)
	require.Equal(t, []string{"12"}, secondTrailer.Get(NextCursorTrailerKey))
}
