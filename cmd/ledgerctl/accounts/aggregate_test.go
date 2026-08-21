package accounts

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHistoricalBalanceSelectorFromFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		at           string
		temporality  string
		checkpointID string
		want         servicepb.HistoricalBalanceTemporality
		wantMicros   uint64
		wantNil      bool
		wantError    string
	}{
		{name: "live read", wantNil: true},
		{name: "effective by default", at: "2026-01-15T12:34:56.123456Z", want: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE, wantMicros: uint64(time.Date(2026, time.January, 15, 12, 34, 56, 123456000, time.UTC).UnixMicro())},
		{name: "insertion", at: "2026-01-15T13:34:56.123456+01:00", temporality: "insertion", want: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION, wantMicros: uint64(time.Date(2026, time.January, 15, 12, 34, 56, 123456000, time.UTC).UnixMicro())},
		{name: "epoch", at: "1970-01-01T00:00:00Z", want: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE},
		{name: "temporality requires at", temporality: "insertion", wantError: "--temporality requires --at"},
		{name: "checkpoint incompatible", at: "2026-01-15T12:34:56Z", checkpointID: "42", wantError: "--at and --checkpoint-id are mutually exclusive"},
		{name: "invalid timestamp", at: "yesterday", wantError: "expected RFC3339 timestamp"},
		{name: "pre epoch", at: "1969-12-31T23:59:59.999999Z", wantError: "timestamps before"},
		{name: "invalid temporality", at: "2026-01-15T12:34:56Z", temporality: "eventual", wantError: "expected effective or insertion"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cmd := NewAggregateVolumesCommand()
			if test.at != "" {
				require.NoError(t, cmd.Flags().Set("at", test.at))
			}
			if test.temporality != "" {
				require.NoError(t, cmd.Flags().Set("temporality", test.temporality))
			}
			if test.checkpointID != "" {
				require.NoError(t, cmd.Flags().Set("checkpoint-id", test.checkpointID))
			}

			selector, err := historicalBalanceSelectorFromFlags(cmd)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				require.Nil(t, selector)

				return
			}
			require.NoError(t, err)
			if test.wantNil {
				require.Nil(t, selector)

				return
			}
			require.Equal(t, test.want, selector.GetTemporality())
			require.Equal(t, test.wantMicros, selector.GetAt().GetData())
		})
	}
}

func TestBuildAggregateVolumesRequestIncludesHistoricalBalanceSelector(t *testing.T) {
	t.Parallel()

	selector := &servicepb.HistoricalBalanceSelector{At: testTimestamp(time.Now()), Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION}
	filter := &commonpb.QueryFilter{}
	request := buildAggregateVolumesRequest("ledger", filter, 123, 0, selector)
	require.Equal(t, "ledger", request.GetLedger())
	require.Same(t, filter, request.GetFilter())
	require.Equal(t, uint64(123), request.GetMinLogSequence())
	require.Same(t, selector, request.GetHistoricalBalance())
}

func TestHistoricalBalanceViewFromTrailer(t *testing.T) {
	t.Parallel()

	valid := &servicepb.HistoricalBalanceView{
		RequestedAt: testTimestamp(time.Now()), Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
		Ledger: "ledger", AuditWatermark: 91, LogWatermark: 87, ManifestVersion: 4,
		ViewToken: "view-token",
	}
	encoded, err := valid.MarshalVT()
	require.NoError(t, err)
	view, err := historicalBalanceViewFromTrailer(metadata.Pairs(historicalBalanceViewTrailerKey, string(encoded)))
	require.NoError(t, err)
	require.Equal(t, valid, view)

	_, err = historicalBalanceViewFromTrailer(nil)
	require.ErrorContains(t, err, "expected exactly one")
	unknown := valid.CloneVT()
	unknown.Temporality = servicepb.HistoricalBalanceTemporality(99)
	encoded, err = unknown.MarshalVT()
	require.NoError(t, err)
	_, err = historicalBalanceViewFromTrailer(metadata.Pairs(historicalBalanceViewTrailerKey, string(encoded)))
	require.ErrorContains(t, err, "unknown temporality")
}

func TestValidateAndEmitHistoricalBalanceView(t *testing.T) {
	t.Parallel()

	selector := &servicepb.HistoricalBalanceSelector{At: &commonpb.Timestamp{Data: 123}, Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION}
	view := &servicepb.HistoricalBalanceView{
		RequestedAt: &commonpb.Timestamp{Data: 123}, Temporality: selector.GetTemporality(), Ledger: "ledger",
		ViewToken: "token",
	}
	require.NoError(t, validateHistoricalBalanceView(selector, view))

	mismatched := view.CloneVT()
	mismatched.Temporality = servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE
	require.ErrorContains(t, validateHistoricalBalanceView(selector, mismatched), "does not match requested temporality")

	cmd := NewAggregateVolumesCommand()
	stderr := &bytes.Buffer{}
	cmd.SetErr(stderr)
	require.NoError(t, emitHistoricalBalanceView(cmd, view))
	line := strings.TrimSpace(stderr.String())
	require.True(t, strings.HasPrefix(line, "historical_balance_view="))
	var display historicalBalanceViewDisplay
	require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "historical_balance_view=")), &display))
	require.Equal(t, "insertion", display.Temporality)
	require.Equal(t, "ledger", display.Ledger)
}

func testTimestamp(value time.Time) *commonpb.Timestamp {
	return &commonpb.Timestamp{Data: uint64(value.UnixMicro())}
}
