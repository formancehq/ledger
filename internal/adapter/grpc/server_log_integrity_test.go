package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/application/logintegrity"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/rafttransportpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestLogIntegrityServiceAppliedHorizonAndDataLoss(t *testing.T) {
	t.Parallel()
	store, err := dal.NewStore(t.TempDir(), noopLogger{}, noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	waited := uint64(0)
	service := NewLogIntegrityServiceServer(store, func(_ context.Context, index uint64) error {
		waited = index

		return nil
	})
	req := &rafttransportpb.GetRangeDigestRequest{FirstLogSequence: 1, LastLogSequence: 1, MinAppliedIndex: 5}
	_, err = service.GetRangeDigest(t.Context(), req)
	require.Equal(t, codes.Unavailable, status.Code(err), "a waiter alone does not prove the pinned snapshot is caught up")
	require.Equal(t, uint64(5), waited)

	batch := store.OpenWriteSession()
	require.NoError(t, state.SetAppliedIndex(batch, 5))
	require.NoError(t, batch.Commit())
	_, err = service.GetRangeDigest(t.Context(), req)
	require.Equal(t, codes.NotFound, status.Code(err), "an empty log has a distinct status")

	batch = store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(1).Build()
	require.NoError(t, batch.SetProto(key, &commonpb.Log{Sequence: 1}))
	require.NoError(t, batch.Commit())
	response, err := service.GetRangeDigest(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, uint64(5), response.GetAppliedIndex())
	require.Equal(t, uint64(1), response.GetLastLogSequenceOnNode())
	require.Len(t, response.GetDigest(), logintegrity.DigestSize)

	batch = store.OpenWriteSession()
	require.NoError(t, batch.SetProto(key, &commonpb.Log{Sequence: 2}))
	require.NoError(t, batch.Commit())
	_, err = service.GetRangeDigest(t.Context(), req)
	require.Equal(t, codes.DataLoss, status.Code(err))
}

func TestLogIntegrityServiceValidatesBeforeWaiting(t *testing.T) {
	t.Parallel()
	service := NewLogIntegrityServiceServer(nil, func(context.Context, uint64) error {
		t.Fatal("invalid ranges must not wait")

		return nil
	})
	_, err := service.GetRangeDigest(t.Context(), &rafttransportpb.GetRangeDigestRequest{
		FirstLogSequence: 0, LastLogSequence: 1, MinAppliedIndex: 100,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestLogIntegrityServiceTreatsApplyLagAsUnavailable(t *testing.T) {
	t.Parallel()
	service := NewLogIntegrityServiceServer(nil, func(context.Context, uint64) error {
		return context.DeadlineExceeded
	})
	_, err := service.GetRangeDigest(t.Context(), &rafttransportpb.GetRangeDigestRequest{
		FirstLogSequence: 1, LastLogSequence: 1, MinAppliedIndex: 5,
	})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
}
