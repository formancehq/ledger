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

func TestPointInTimeSelectorFromFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		pit              string
		useInsertionDate bool
		checkpointID     string
		wantAxis         servicepb.PointInTimeAxis
		wantMicros       uint64
		wantNil          bool
		wantError        string
	}{
		{
			name:    "live read",
			wantNil: true,
		},
		{
			name:       "effective axis by default",
			pit:        "2026-01-15T12:34:56.123456Z",
			wantAxis:   servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_EFFECTIVE,
			wantMicros: uint64(time.Date(2026, time.January, 15, 12, 34, 56, 123456000, time.UTC).UnixMicro()),
		},
		{
			name:             "insertion axis",
			pit:              "2026-01-15T13:34:56.123456+01:00",
			useInsertionDate: true,
			wantAxis:         servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_INSERTION,
			wantMicros:       uint64(time.Date(2026, time.January, 15, 12, 34, 56, 123456000, time.UTC).UnixMicro()),
		},
		{
			name:       "Unix epoch is accepted",
			pit:        "1970-01-01T00:00:00Z",
			wantAxis:   servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_EFFECTIVE,
			wantMicros: 0,
		},
		{
			name:             "insertion axis requires PIT",
			useInsertionDate: true,
			wantError:        "--use-insertion-date requires --pit",
		},
		{
			name:         "checkpoint is incompatible",
			pit:          "2026-01-15T12:34:56Z",
			checkpointID: "42",
			wantError:    "--pit and --checkpoint-id are mutually exclusive",
		},
		{
			name:      "invalid timestamp",
			pit:       "yesterday",
			wantError: "expected RFC3339 timestamp",
		},
		{
			name:      "pre epoch timestamp",
			pit:       "1969-12-31T23:59:59.999999Z",
			wantError: "timestamps before 1970-01-01T00:00:00Z are not supported",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cmd := NewAggregateVolumesCommand()
			if test.pit != "" {
				require.NoError(t, cmd.Flags().Set("pit", test.pit))
			}
			if test.useInsertionDate {
				require.NoError(t, cmd.Flags().Set("use-insertion-date", "true"))
			}
			if test.checkpointID != "" {
				require.NoError(t, cmd.Flags().Set("checkpoint-id", test.checkpointID))
			}

			selector, err := pointInTimeSelectorFromFlags(cmd)
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

			require.NotNil(t, selector)
			require.Equal(t, test.wantAxis, selector.GetAxis())
			require.Equal(t, test.wantMicros, selector.GetAt().GetData())
		})
	}
}

func TestBuildAggregateVolumesRequestIncludesPointInTimeSelector(t *testing.T) {
	t.Parallel()

	selector := &servicepb.PointInTimeSelector{
		At:   testTimestamp(time.Date(2026, time.March, 4, 5, 6, 7, 8000, time.UTC)),
		Axis: servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_INSERTION,
	}
	filter := &commonpb.QueryFilter{}

	request := buildAggregateVolumesRequest("ledger", filter, 123, 0, selector)

	require.Equal(t, "ledger", request.GetLedger())
	require.Same(t, filter, request.GetFilter())
	require.Equal(t, uint64(123), request.GetMinLogSequence())
	require.Zero(t, request.GetCheckpointId())
	require.Same(t, selector, request.GetPointInTime())
}

func TestPointInTimeViewFromTrailer(t *testing.T) {
	t.Parallel()

	validView := &servicepb.PointInTimeView{
		RequestedAt:          testTimestamp(time.Date(2026, time.January, 15, 12, 0, 0, 0, time.UTC)),
		Axis:                 servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_INSERTION,
		LedgerId:             7,
		AuditWatermark:       91,
		LogWatermark:         87,
		ManifestVersion:      4,
		HistoryAvailableFrom: testTimestamp(time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)),
		ViewToken:            "view-token",
	}

	encode := func(t *testing.T, view *servicepb.PointInTimeView) string {
		t.Helper()

		encoded, err := view.MarshalVT()
		require.NoError(t, err)

		return string(encoded)
	}

	tests := []struct {
		name      string
		trailer   func(t *testing.T) metadata.MD
		wantError string
	}{
		{
			name: "valid immutable view",
			trailer: func(t *testing.T) metadata.MD {
				return metadata.Pairs(pointInTimeViewTrailerKey, encode(t, validView))
			},
		},
		{
			name: "missing trailer",
			trailer: func(_ *testing.T) metadata.MD {
				return nil
			},
			wantError: "expected exactly one",
		},
		{
			name: "duplicate trailer",
			trailer: func(t *testing.T) metadata.MD {
				encoded := encode(t, validView)

				return metadata.MD{pointInTimeViewTrailerKey: []string{encoded, encoded}}
			},
			wantError: "got 2",
		},
		{
			name: "malformed trailer",
			trailer: func(_ *testing.T) metadata.MD {
				return metadata.Pairs(pointInTimeViewTrailerKey, "\xff")
			},
			wantError: "decoding",
		},
		{
			name: "missing token fails closed",
			trailer: func(t *testing.T) metadata.MD {
				view := validView.CloneVT()
				view.ViewToken = ""

				return metadata.Pairs(pointInTimeViewTrailerKey, encode(t, view))
			},
			wantError: "incomplete",
		},
		{
			name: "unknown axis fails closed",
			trailer: func(t *testing.T) metadata.MD {
				view := validView.CloneVT()
				view.Axis = servicepb.PointInTimeAxis(99)

				return metadata.Pairs(pointInTimeViewTrailerKey, encode(t, view))
			},
			wantError: "unknown axis 99",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			view, err := pointInTimeViewFromTrailer(test.trailer(t))
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				require.Nil(t, view)

				return
			}

			require.NoError(t, err)
			require.Equal(t, validView, view)
		})
	}
}

func TestValidatePointInTimeView(t *testing.T) {
	t.Parallel()

	selector := &servicepb.PointInTimeSelector{
		At:   &commonpb.Timestamp{Data: 123},
		Axis: servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_INSERTION,
	}
	view := &servicepb.PointInTimeView{
		RequestedAt:          &commonpb.Timestamp{Data: 123},
		Axis:                 servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_INSERTION,
		HistoryAvailableFrom: &commonpb.Timestamp{},
		ViewToken:            "token",
	}

	require.NoError(t, validatePointInTimeView(selector, view))

	mismatchedTimestamp := view.CloneVT()
	mismatchedTimestamp.RequestedAt = &commonpb.Timestamp{Data: 124}
	require.ErrorContains(t, validatePointInTimeView(selector, mismatchedTimestamp), "does not match requested timestamp")

	mismatchedAxis := view.CloneVT()
	mismatchedAxis.Axis = servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_EFFECTIVE
	require.ErrorContains(t, validatePointInTimeView(selector, mismatchedAxis), "does not match requested axis")
}

func TestEmitPointInTimeView(t *testing.T) {
	t.Parallel()

	cmd := NewAggregateVolumesCommand()
	stderr := &bytes.Buffer{}
	cmd.SetErr(stderr)

	view := &servicepb.PointInTimeView{
		RequestedAt:          testTimestamp(time.Date(2026, time.January, 15, 12, 0, 0, 123456000, time.FixedZone("CET", 3600))),
		Axis:                 servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_INSERTION,
		LedgerId:             7,
		AuditWatermark:       91,
		LogWatermark:         87,
		ManifestVersion:      4,
		HistoryAvailableFrom: testTimestamp(time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)),
		ViewToken:            "view-token",
	}

	require.NoError(t, emitPointInTimeView(cmd, view))

	line := strings.TrimSpace(stderr.String())
	require.True(t, strings.HasPrefix(line, "point_in_time_view="))

	var display pointInTimeViewDisplay
	require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "point_in_time_view=")), &display))
	require.Equal(t, pointInTimeViewDisplay{
		RequestedAt:          "2026-01-15T11:00:00.123456Z",
		Axis:                 "insertion",
		LedgerID:             7,
		AuditWatermark:       91,
		LogWatermark:         87,
		ManifestVersion:      4,
		HistoryAvailableFrom: "2025-01-01T00:00:00Z",
		ViewToken:            "view-token",
	}, display)
}

func testTimestamp(value time.Time) *commonpb.Timestamp {
	return &commonpb.Timestamp{Data: uint64(value.UnixMicro())}
}
