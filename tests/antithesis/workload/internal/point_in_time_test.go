package internal

import (
	"errors"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestDecodePointInTimeViewTrailer(t *testing.T) {
	t.Parallel()

	view := &servicepb.PointInTimeView{
		RequestedAt:          &commonpb.Timestamp{Data: 42},
		Axis:                 servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_EFFECTIVE,
		LedgerId:             7,
		AuditWatermark:       11,
		LogWatermark:         9,
		ManifestVersion:      3,
		HistoryAvailableFrom: &commonpb.Timestamp{Data: 1},
		ViewToken:            "view-token",
	}
	encoded, err := view.MarshalVT()
	require.NoError(t, err)

	decoded, err := decodePointInTimeViewTrailer(metadata.Pairs(
		pointInTimeViewTrailerKey,
		string(encoded),
	))
	require.NoError(t, err)
	require.Equal(t, view, decoded)
}

func TestValidatePointInTimeViewAuthenticatesLedgerIncarnation(t *testing.T) {
	t.Parallel()

	request := &servicepb.AggregateVolumesRequest{PointInTime: &servicepb.PointInTimeSelector{
		At:   &commonpb.Timestamp{Data: 42},
		Axis: servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_EFFECTIVE,
	}}
	view := &servicepb.PointInTimeView{
		RequestedAt: &commonpb.Timestamp{Data: 42},
		Axis:        servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_EFFECTIVE,
		LedgerId:    8,
	}

	err := validatePointInTimeView(request, 7, view)
	require.EqualError(t, err, "point-in-time view ledger ID 8 differs from expected incarnation 7")
}

func TestIsClassifiedPointInTimeFailure(t *testing.T) {
	t.Parallel()

	for _, reason := range []string{
		"HISTORY_BUILDING",
		"HISTORY_BEHIND",
		"HISTORY_EXPIRED",
		"HISTORY_SOURCE_MISSING",
		"HISTORY_CORRUPT",
	} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()

			st, err := status.New(codes.Internal, "point-in-time read failed").WithDetails(
				&errdetails.ErrorInfo{Reason: reason},
			)
			require.NoError(t, err)
			require.True(t, IsClassifiedPointInTimeFailure(st.Err()))
		})
	}

	require.False(t, IsClassifiedPointInTimeFailure(errors.New("plain failure")))
}

func TestDecodePointInTimeViewTrailerRejectsIncompleteView(t *testing.T) {
	t.Parallel()

	encoded, err := (&servicepb.PointInTimeView{}).MarshalVT()
	require.NoError(t, err)

	_, err = decodePointInTimeViewTrailer(metadata.Pairs(
		pointInTimeViewTrailerKey,
		string(encoded),
	))
	require.EqualError(t, err, "point-in-time view trailer is incomplete")
}

func TestCanonicalFlatAggregate(t *testing.T) {
	t.Parallel()

	got, err := CanonicalFlatAggregate(&commonpb.AggregateResult{Volumes: []*commonpb.AggregatedVolume{
		{
			Asset:  "USD/4",
			Color:  "RED",
			Input:  commonpb.NewUint256FromUint64(100),
			Output: commonpb.NewUint256FromUint64(25),
		},
		{
			Asset:  "EUR/2",
			Input:  commonpb.NewUint256FromUint64(50),
			Output: commonpb.NewUint256FromUint64(50),
		},
	}})
	require.NoError(t, err)
	require.Equal(t, []CanonicalVolume{
		{Asset: "EUR/2", Input: "50", Output: "50"},
		{Asset: "USD/4", Color: "RED", Input: "100", Output: "25"},
	}, got)
}

func TestCanonicalFlatAggregateRejectsDuplicateBucket(t *testing.T) {
	t.Parallel()

	_, err := CanonicalFlatAggregate(&commonpb.AggregateResult{Volumes: []*commonpb.AggregatedVolume{
		{Asset: "USD", Color: "RED"},
		{Asset: "USD", Color: "RED"},
	}})
	require.EqualError(t, err, `aggregate contains duplicate bucket asset="USD" color="RED"`)
}
