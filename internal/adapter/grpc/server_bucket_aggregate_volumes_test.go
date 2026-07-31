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
	trailer metadata.MD
}

func (s *aggregateVolumesTransportStream) Method() string {
	return "/formance.ledger.v3.BucketService/AggregateVolumes"
}

func (s *aggregateVolumesTransportStream) SetHeader(metadata.MD) error {
	return nil
}

func (s *aggregateVolumesTransportStream) SendHeader(metadata.MD) error {
	return nil
}

func (s *aggregateVolumesTransportStream) SetTrailer(md metadata.MD) error {
	s.trailer = metadata.Join(s.trailer, md)

	return nil
}

func TestAggregateVolumesPointInTimeServer(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		protoAxis servicepb.PointInTimeAxis
		storeAxis balancehistorystore.Axis
	}{
		{
			name:      "effective axis",
			protoAxis: servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_EFFECTIVE,
			storeAxis: balancehistorystore.AxisEffective,
		},
		{
			name:      "insertion axis",
			protoAxis: servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_INSERTION,
			storeAxis: balancehistorystore.AxisInsertion,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			impl, controller := newListHandlerHarness(t)
			filter := &commonpb.QueryFilter{}
			view := &appctrl.VolumeViewToken{
				RequestedAt:          1_704_164_645_123_456,
				Axis:                 tc.storeAxis,
				LedgerID:             17,
				AuditWatermark:       99,
				LogWatermark:         88,
				ManifestVersion:      7,
				HistoryAvailableFrom: 1_600_000_000_000_000,
				Token:                "immutable-view-token",
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
					require.Equal(t, &appctrl.PointInTimeSelector{
						At:   view.RequestedAt,
						Axis: tc.storeAxis,
					}, read.PointInTime)

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
				PointInTime: &servicepb.PointInTimeSelector{
					At:   &commonpb.Timestamp{Data: view.RequestedAt},
					Axis: tc.protoAxis,
				},
			})

			require.NoError(t, err)
			require.Same(t, aggregate, result)
			decodedView, err := pointInTimeViewFromMetadata(transport.trailer)
			require.NoError(t, err)
			require.Equal(t, view, decodedView)
		})
	}
}

func TestAggregateVolumesPointInTimeServerRejectsInvalidSelectors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		req  *servicepb.AggregateVolumesRequest
	}{
		{
			name: "timestamp absent",
			req: &servicepb.AggregateVolumesRequest{
				Ledger:      "main",
				PointInTime: &servicepb.PointInTimeSelector{},
			},
		},
		{
			name: "unknown axis",
			req: &servicepb.AggregateVolumesRequest{
				Ledger: "main",
				PointInTime: &servicepb.PointInTimeSelector{
					At:   &commonpb.Timestamp{Data: 1},
					Axis: servicepb.PointInTimeAxis(99),
				},
			},
		},
		{
			name: "checkpoint and point in time",
			req: &servicepb.AggregateVolumesRequest{
				Ledger:       "main",
				CheckpointId: 3,
				PointInTime: &servicepb.PointInTimeSelector{
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

func TestAggregateVolumesPointInTimeServerRequiresViewToken(t *testing.T) {
	t.Parallel()

	impl, controller := newListHandlerHarness(t)
	controller.EXPECT().AggregateVolumes(gomock.Any(), "main", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&appctrl.AggregateVolumesResult{Aggregate: &commonpb.AggregateResult{}}, nil)

	_, err := impl.AggregateVolumes(context.Background(), &servicepb.AggregateVolumesRequest{
		Ledger: "main",
		PointInTime: &servicepb.PointInTimeSelector{
			At: &commonpb.Timestamp{Data: 1},
		},
	})

	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestAggregateVolumesPointInTimeServerRejectsMismatchedView(t *testing.T) {
	t.Parallel()

	selector := &servicepb.PointInTimeSelector{
		At:   &commonpb.Timestamp{Data: 10},
		Axis: servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_EFFECTIVE,
	}
	for _, tc := range []struct {
		name string
		view *appctrl.VolumeViewToken
	}{
		{
			name: "requested timestamp",
			view: &appctrl.VolumeViewToken{
				RequestedAt: 11,
				Axis:        balancehistorystore.AxisEffective,
				Token:       "wrong-timestamp-view",
			},
		},
		{
			name: "axis",
			view: &appctrl.VolumeViewToken{
				RequestedAt: 10,
				Axis:        balancehistorystore.AxisInsertion,
				Token:       "wrong-axis-view",
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
				Ledger:      "main",
				PointInTime: selector,
			})

			require.Nil(t, result)
			require.ErrorContains(t, err, "does not match requested selector")
			require.Equal(t, codes.Internal, status.Code(err))
			require.Empty(t, transport.trailer)
		})
	}
}

func TestPointInTimeViewMetadataRejectsUnknownAxis(t *testing.T) {
	t.Parallel()

	trailer, err := pointInTimeViewMetadata(&appctrl.VolumeViewToken{
		Axis:  balancehistorystore.Axis(99),
		Token: "must-not-be-serialized",
	})

	require.Nil(t, trailer)
	require.ErrorContains(t, err, "unknown point-in-time axis value 99")
}

func TestPointInTimeViewMetadataRejectsEmptyToken(t *testing.T) {
	t.Parallel()

	trailer, err := pointInTimeViewMetadata(&appctrl.VolumeViewToken{
		Axis: balancehistorystore.AxisEffective,
	})

	require.Nil(t, trailer)
	require.ErrorContains(t, err, "immutable view token is empty")
}
