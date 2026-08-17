package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	appctrl "github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

type aggregateVolumesTransportStream struct {
	header     metadata.MD
	headerSent chan struct{}
	trailer    metadata.MD
}

func (s *aggregateVolumesTransportStream) Method() string {
	return "/formance.ledger.v3.BucketService/AggregateVolumes"
}

func (s *aggregateVolumesTransportStream) SetHeader(metadata.MD) error {
	return nil
}

func (s *aggregateVolumesTransportStream) SendHeader(md metadata.MD) error {
	s.header = metadata.Join(s.header, md)
	if s.headerSent != nil {
		select {
		case s.headerSent <- struct{}{}:
		default:
		}
	}

	return nil
}

func (s *aggregateVolumesTransportStream) SetTrailer(md metadata.MD) error {
	s.trailer = metadata.Join(s.trailer, md)

	return nil
}

func TestAggregateVolumesHistoricalBalanceServer(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name             string
		protoTemporality servicepb.HistoricalBalanceTemporality
		storeTemporality balancehistorystore.Temporality
	}{
		{
			name:             "effective temporality",
			protoTemporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			storeTemporality: balancehistorystore.TemporalityEffective,
		},
		{
			name:             "insertion temporality",
			protoTemporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
			storeTemporality: balancehistorystore.TemporalityInsertion,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			impl, controller := newListHandlerHarness(t)
			filter := &commonpb.QueryFilter{}
			view := &appctrl.HistoricalBalanceViewToken{
				RequestedAt:     1_704_164_645_123_456,
				Temporality:     tc.storeTemporality,
				Ledger:          "main",
				AuditWatermark:  99,
				LogWatermark:    88,
				ManifestVersion: 7,
				Token:           "immutable-view-token",
			}
			aggregate := &commonpb.AggregateResult{
				Volumes: []*commonpb.AggregatedVolume{{
					Asset: "USD/2",
					Input: commonpb.NewUint256FromUint64(12),
				}},
			}

			controller.EXPECT().AggregateVolumes(gomock.Any(), "main", filter, gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					_ string,
					_ *commonpb.QueryFilter,
					opts query.AggregateOptions,
					read appctrl.AggregateVolumesReadOptions,
				) (*appctrl.AggregateVolumesResult, error) {
					require.Equal(t, query.AggregateOptions{
						UseMaxPrecision: true,
						GroupByPrefixes: []string{"users:"},
						CollapseColors:  true,
					}, opts)
					require.Equal(t, uint64(42), read.MinLogSequence)
					require.Equal(t, &appctrl.HistoricalBalanceSelector{
						At:          view.RequestedAt,
						Temporality: tc.storeTemporality,
					}, read.HistoricalBalance)

					return &appctrl.AggregateVolumesResult{Aggregate: aggregate, View: view}, nil
				})

			transport := &aggregateVolumesTransportStream{}
			ctx := grpc.NewContextWithServerTransportStream(context.Background(), transport)
			result, err := impl.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
				Ledger:          "main",
				Filter:          filter,
				MinLogSequence:  42,
				UseMaxPrecision: true,
				GroupByPrefixes: []string{"users:"},
				CollapseColors:  true,
				HistoricalBalance: &servicepb.HistoricalBalanceSelector{
					At:          &commonpb.Timestamp{Data: view.RequestedAt},
					Temporality: tc.protoTemporality,
				},
			})

			require.NoError(t, err)
			require.Same(t, aggregate, result)
			decodedView, err := historicalBalanceViewFromMetadata(transport.trailer)
			require.NoError(t, err)
			require.Equal(t, view, decodedView)
		})
	}
}

func TestAggregateVolumesHistoricalBalanceServerRejectsInvalidSelectors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		req  *servicepb.AggregateVolumesRequest
	}{
		{
			name: "timestamp absent",
			req: &servicepb.AggregateVolumesRequest{
				Ledger:            "main",
				HistoricalBalance: &servicepb.HistoricalBalanceSelector{},
			},
		},
		{
			name: "unknown temporality",
			req: &servicepb.AggregateVolumesRequest{
				Ledger: "main",
				HistoricalBalance: &servicepb.HistoricalBalanceSelector{
					At:          &commonpb.Timestamp{Data: 1},
					Temporality: servicepb.HistoricalBalanceTemporality(99),
				},
			},
		},
		{
			name: "checkpoint and historical balance",
			req: &servicepb.AggregateVolumesRequest{
				Ledger:       "main",
				CheckpointId: 3,
				HistoricalBalance: &servicepb.HistoricalBalanceSelector{
					At: &commonpb.Timestamp{Data: 1},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			impl, _ := newListHandlerHarness(t)
			_, err := impl.AggregateVolumes(context.Background(), tc.req)

			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestAggregateVolumesHistoricalBalanceServerRequiresViewToken(t *testing.T) {
	t.Parallel()

	impl, controller := newListHandlerHarness(t)
	controller.EXPECT().AggregateVolumes(gomock.Any(), "main", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&appctrl.AggregateVolumesResult{Aggregate: &commonpb.AggregateResult{}}, nil)

	_, err := impl.AggregateVolumes(context.Background(), &servicepb.AggregateVolumesRequest{
		Ledger: "main",
		HistoricalBalance: &servicepb.HistoricalBalanceSelector{
			At: &commonpb.Timestamp{Data: 1},
		},
	})

	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestAggregateVolumesHistoricalBalanceServerRejectsMismatchedView(t *testing.T) {
	t.Parallel()

	selector := &servicepb.HistoricalBalanceSelector{
		At:          &commonpb.Timestamp{Data: 10},
		Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
	}
	for _, tc := range []struct {
		name string
		view *appctrl.HistoricalBalanceViewToken
	}{
		{
			name: "requested timestamp",
			view: &appctrl.HistoricalBalanceViewToken{
				RequestedAt: 11,
				Temporality: balancehistorystore.TemporalityEffective,
				Token:       "wrong-timestamp-view",
			},
		},
		{
			name: "temporality",
			view: &appctrl.HistoricalBalanceViewToken{
				RequestedAt: 10,
				Temporality: balancehistorystore.TemporalityInsertion,
				Token:       "wrong-temporality-view",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			impl, controller := newListHandlerHarness(t)
			controller.EXPECT().AggregateVolumes(gomock.Any(), "main", gomock.Any(), gomock.Any(), gomock.Any()).
				Return(&appctrl.AggregateVolumesResult{
					Aggregate: &commonpb.AggregateResult{},
					View:      tc.view,
				}, nil)
			transport := &aggregateVolumesTransportStream{}
			ctx := grpc.NewContextWithServerTransportStream(context.Background(), transport)

			result, err := impl.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
				Ledger:            "main",
				HistoricalBalance: selector,
			})

			require.Nil(t, result)
			require.ErrorContains(t, err, "does not match requested selector")
			require.Equal(t, codes.Internal, status.Code(err))
			require.Empty(t, transport.trailer)
		})
	}
}

func TestHistoricalBalanceViewMetadataRejectsUnknownTemporality(t *testing.T) {
	t.Parallel()

	trailer, err := historicalBalanceViewMetadata(&appctrl.HistoricalBalanceViewToken{
		Temporality: balancehistorystore.Temporality(99),
		Token:       "must-not-be-serialized",
	})

	require.Nil(t, trailer)
	require.ErrorContains(t, err, "unknown historical-balance temporality value 99")
}

func TestHistoricalBalanceViewMetadataRejectsEmptyToken(t *testing.T) {
	t.Parallel()

	trailer, err := historicalBalanceViewMetadata(&appctrl.HistoricalBalanceViewToken{
		Temporality: balancehistorystore.TemporalityEffective,
	})

	require.Nil(t, trailer)
	require.ErrorContains(t, err, "immutable view token is empty")
}
