package internal

import (
	"context"
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

func TestIsLinearizablePITPartitionTransient(t *testing.T) {
	t.Parallel()

	historyBehind, err := status.New(codes.Unavailable, "behind").WithDetails(
		&errdetails.ErrorInfo{Reason: "HISTORY_BEHIND"},
	)
	require.NoError(t, err)
	externalService, err := status.New(codes.Unavailable, "dependency").WithDetails(
		&errdetails.ErrorInfo{Reason: "EXTERNAL_SERVICE_ERROR"},
	)
	require.NoError(t, err)

	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "raft unavailable", err: status.Error(codes.Unavailable, "no leader"), want: true},
		{name: "history behind", err: historyBehind.Err(), want: true},
		{name: "wire deadline", err: status.Error(codes.DeadlineExceeded, "deadline"), want: true},
		{name: "external service", err: externalService.Err(), want: false},
		{name: "internal", err: status.Error(codes.Internal, "corrupt"), want: false},
		{name: "canceled", err: status.Error(codes.Canceled, "teardown"), want: false},
		{name: "plain", err: errors.New("plain"), want: false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, testCase.want, IsLinearizablePITPartitionTransient(testCase.err))
		})
	}
}

func TestWithLinearizablePITProbeOmitsConsistencyAndReplacesProbe(t *testing.T) {
	t.Parallel()

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		metadataKeyConsistency,
		"stale",
		LinearizablePITProbeMetadataKey,
		"old",
		"unrelated",
		"kept",
	))
	ctx = withLinearizablePITProbe(ctx, "fresh")

	outgoing, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	require.Empty(t, outgoing.Get(metadataKeyConsistency))
	require.Equal(t, []string{"fresh"}, outgoing.Get(LinearizablePITProbeMetadataKey))
	require.Equal(t, []string{"kept"}, outgoing.Get("unrelated"))
}

func TestLinearizablePITBarrierReachedRequiresExactProbeHeader(t *testing.T) {
	t.Parallel()

	require.True(t, linearizablePITBarrierReached(metadata.Pairs(
		LinearizablePITBarrierReachedMetadataKey,
		"probe-1",
	), "probe-1"))
	require.False(t, linearizablePITBarrierReached(metadata.Pairs(
		LinearizablePITBarrierReachedMetadataKey,
		"other",
	), "probe-1"))
	require.False(t, linearizablePITBarrierReached(metadata.Pairs(
		LinearizablePITBarrierReachedMetadataKey,
		"probe-1",
		LinearizablePITBarrierReachedMetadataKey,
		"probe-1",
	), "probe-1"))
}

func TestValidateLinearizablePITResult(t *testing.T) {
	t.Parallel()

	fixture := &LinearizablePITFixture{
		MinLogSequence: 7,
		Expected: []CanonicalVolume{{
			Asset:  "COIN",
			Input:  "11",
			Output: "11",
		}},
	}
	result := &commonpb.AggregateResult{Volumes: []*commonpb.AggregatedVolume{{
		Asset:  "COIN",
		Input:  commonpb.NewUint256FromUint64(11),
		Output: commonpb.NewUint256FromUint64(11),
	}}}

	require.NoError(t, validateLinearizablePITResult(
		result,
		&servicepb.HistoricalBalanceView{LogWatermark: 7},
		fixture,
	))
	require.ErrorContains(t, validateLinearizablePITResult(
		result,
		&servicepb.HistoricalBalanceView{LogWatermark: 6},
		fixture,
	), "below acknowledged marker")
}
