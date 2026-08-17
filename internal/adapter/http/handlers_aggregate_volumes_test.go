package http

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	appctrl "github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

func aggregateVolumesResult(result *commonpb.AggregateResult) *appctrl.AggregateVolumesResult {
	return &appctrl.AggregateVolumesResult{Aggregate: result}
}

func TestHandleAggregateVolumes_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ledgerName string, filter *commonpb.QueryFilter, opts query.AggregateOptions, read appctrl.AggregateVolumesReadOptions) (*appctrl.AggregateVolumesResult, error) {
			require.Equal(t, "my-ledger", ledgerName)
			require.Nil(t, filter)
			require.False(t, opts.UseMaxPrecision)
			require.Empty(t, opts.GroupByPrefixes)
			require.Nil(t, read.HistoricalBalance)

			return aggregateVolumesResult(&commonpb.AggregateResult{
				Volumes: []*commonpb.AggregatedVolume{
					{
						Asset:  "USD/2",
						Input:  commonpb.NewUint256FromUint64(1000),
						Output: commonpb.NewUint256FromUint64(400),
					},
				},
			}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/volumes", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAggregateVolumes(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	wrapper := decodeResponse[BaseResponse[aggregateVolumesResponseJSON]](t, w)
	resp := wrapper.Data
	require.Len(t, resp.Volumes, 1)
	require.Equal(t, "USD/2", resp.Volumes[0].Asset)
	require.Equal(t, "1000", resp.Volumes[0].Input)
	require.Equal(t, "400", resp.Volumes[0].Output)
	require.Equal(t, "600", resp.Volumes[0].Balance)
	require.Empty(t, resp.Groups)
	require.Empty(t, w.Header().Get(historicalBalanceViewHeader))
}

func TestHandleAggregateVolumes_WithOptions(t *testing.T) {
	t.Parallel()

	var capturedOpts query.AggregateOptions
	var capturedFilter *commonpb.QueryFilter

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, filter *commonpb.QueryFilter, opts query.AggregateOptions, _ appctrl.AggregateVolumesReadOptions) (*appctrl.AggregateVolumesResult, error) {
			capturedOpts = opts
			capturedFilter = filter

			return aggregateVolumesResult(&commonpb.AggregateResult{}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet,
		"/my-ledger/volumes?useMaxPrecision=true&collapseColors=true&groupByPrefixes=users:,merchants:&filter="+
			url.QueryEscape(`address ^= "users:"`), nil, map[string]string{
			"ledgerName": "my-ledger",
		})

	srv.handleAggregateVolumes(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, capturedOpts.UseMaxPrecision)
	require.True(t, capturedOpts.CollapseColors)
	require.Equal(t, []string{"users:", "merchants:"}, capturedOpts.GroupByPrefixes)
	require.NotNil(t, capturedFilter)
	require.Equal(t, "users:", capturedFilter.GetAddress().GetHardcodedPrefix())
}

func TestHandleAggregateVolumes_HistoricalBalance(t *testing.T) {
	t.Parallel()

	const pit = "2024-01-02T03:04:05.123456Z"
	parsedPIT, err := time.Parse(time.RFC3339Nano, pit)
	require.NoError(t, err)
	expectedAt := uint64(parsedPIT.UnixMicro())

	for _, tc := range []struct {
		name  string
		query string
		axis  balancehistorystore.Temporality
	}{
		{
			name: "effective by default",
			axis: balancehistorystore.TemporalityEffective,
		},
		{
			name:  "insertion temporality",
			query: "&temporality=insertion",
			axis:  balancehistorystore.TemporalityInsertion,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			view := &appctrl.HistoricalBalanceViewToken{
				RequestedAt:     expectedAt,
				Temporality:     tc.axis,
				Ledger:          "my-ledger",
				AuditWatermark:  99,
				LogWatermark:    88,
				ManifestVersion: 7,
				Token:           "immutable-view-token",
			}
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().AggregateVolumes(gomock.Any(), "my-ledger", gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					_ string,
					filter *commonpb.QueryFilter,
					_ query.AggregateOptions,
					read appctrl.AggregateVolumesReadOptions,
				) (*appctrl.AggregateVolumesResult, error) {
					require.NotNil(t, filter)
					require.Equal(t, "users:", filter.GetAddress().GetHardcodedPrefix())
					require.Equal(t, &appctrl.HistoricalBalanceSelector{At: expectedAt, Temporality: tc.axis}, read.HistoricalBalance)

					return &appctrl.AggregateVolumesResult{
						Aggregate: &commonpb.AggregateResult{},
						View:      view,
					}, nil
				})
			srv := newTestServer(t, backend)

			target := "/my-ledger/volumes?at=" + url.QueryEscape(pit) + tc.query +
				"&filter=" + url.QueryEscape(`address ^= "users:"`)
			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodGet, target, nil, map[string]string{"ledgerName": "my-ledger"})

			srv.handleAggregateVolumes(w, r)

			require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
			rawView, err := base64.StdEncoding.DecodeString(w.Header().Get(historicalBalanceViewHeader))
			require.NoError(t, err)
			wireView := &servicepb.HistoricalBalanceView{}
			require.NoError(t, wireView.UnmarshalVT(rawView))
			require.Equal(t, expectedAt, wireView.GetRequestedAt().GetData())
			require.Equal(t, view.Ledger, wireView.GetLedger())
			require.Equal(t, view.AuditWatermark, wireView.GetAuditWatermark())
			require.Equal(t, view.LogWatermark, wireView.GetLogWatermark())
			require.Equal(t, view.ManifestVersion, wireView.GetManifestVersion())
			require.Equal(t, view.Token, wireView.GetViewToken())
			if tc.axis == balancehistorystore.TemporalityInsertion {
				require.Equal(t, servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION, wireView.GetTemporality())
			} else {
				require.Equal(t, servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE, wireView.GetTemporality())
			}
		})
	}
}

func TestHandleAggregateVolumes_HistoricalBalanceValidation(t *testing.T) {
	t.Parallel()

	const validPIT = "2024-01-02T03:04:05.123456Z"
	for _, tc := range []struct {
		name  string
		query string
	}{
		{
			name:  "invalid timestamp",
			query: "?at=not-a-timestamp",
		},
		{
			name:  "timestamp before epoch",
			query: "?at=" + url.QueryEscape("1969-12-31T23:59:59.999999Z"),
		},
		{
			name:  "temporality without timestamp",
			query: "?temporality=insertion",
		},
		{
			name:  "invalid temporality",
			query: "?at=" + url.QueryEscape(validPIT) + "&temporality=eventual",
		},
		{
			name:  "temporality repeated",
			query: "?at=" + url.QueryEscape(validPIT) + "&temporality=insertion&temporality=insertion",
		},
		{
			name:  "at repeated",
			query: "?at=" + url.QueryEscape(validPIT) + "&at=" + url.QueryEscape(validPIT),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))
			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodGet, "/my-ledger/volumes"+tc.query, nil, map[string]string{
				"ledgerName": "my-ledger",
			})

			srv.handleAggregateVolumes(w, r)

			require.Equal(t, http.StatusBadRequest, w.Code, "body: %s", w.Body.String())
			require.Empty(t, w.Header().Get(historicalBalanceViewHeader))
		})
	}
}

func TestHandleAggregateVolumes_HistoricalBalanceRequiresViewToken(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), "my-ledger", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&appctrl.AggregateVolumesResult{Aggregate: &commonpb.AggregateResult{}}, nil)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/volumes?at=2024-01-02T03:04:05Z", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAggregateVolumes(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Empty(t, w.Header().Get(historicalBalanceViewHeader))
}

func TestHandleAggregateVolumes_HistoricalBalanceRejectsMismatchedView(t *testing.T) {
	t.Parallel()

	const pit = "2024-01-02T03:04:05Z"
	parsedPIT, err := time.Parse(time.RFC3339Nano, pit)
	require.NoError(t, err)
	expectedAt := uint64(parsedPIT.UnixMicro())

	for _, tc := range []struct {
		name string
		view *appctrl.HistoricalBalanceViewToken
	}{
		{
			name: "requested timestamp",
			view: &appctrl.HistoricalBalanceViewToken{
				RequestedAt: expectedAt + 1,
				Temporality: balancehistorystore.TemporalityEffective,
				Token:       "wrong-timestamp-view",
			},
		},
		{
			name: "axis",
			view: &appctrl.HistoricalBalanceViewToken{
				RequestedAt: expectedAt,
				Temporality: balancehistorystore.TemporalityInsertion,
				Token:       "wrong-axis-view",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().AggregateVolumes(gomock.Any(), "my-ledger", gomock.Any(), gomock.Any(), gomock.Any()).
				Return(&appctrl.AggregateVolumesResult{
					Aggregate: &commonpb.AggregateResult{},
					View:      tc.view,
				}, nil)
			srv := newTestServer(t, backend)

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodGet, "/my-ledger/volumes?at="+url.QueryEscape(pit), nil, map[string]string{
				"ledgerName": "my-ledger",
			})

			srv.handleAggregateVolumes(w, r)

			require.Equal(t, http.StatusInternalServerError, w.Code)
			require.Empty(t, w.Header().Get(historicalBalanceViewHeader))
		})
	}
}

func TestHandleAggregateVolumes_HistoricalBalanceRejectsUnknownViewTemporality(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), "my-ledger", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&appctrl.AggregateVolumesResult{
			Aggregate: &commonpb.AggregateResult{},
			View: &appctrl.HistoricalBalanceViewToken{
				Temporality: balancehistorystore.Temporality(99),
				Token:       "must-not-be-serialized",
			},
		}, nil)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/volumes?at=2024-01-02T03:04:05Z", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAggregateVolumes(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Empty(t, w.Header().Get(historicalBalanceViewHeader))
}

func TestHandleAggregateVolumes_HistoricalBalanceRejectsEmptyViewToken(t *testing.T) {
	t.Parallel()

	const pit = "2024-01-02T03:04:05Z"
	parsedPIT, err := time.Parse(time.RFC3339Nano, pit)
	require.NoError(t, err)

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), "my-ledger", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&appctrl.AggregateVolumesResult{
			Aggregate: &commonpb.AggregateResult{},
			View: &appctrl.HistoricalBalanceViewToken{
				RequestedAt: uint64(parsedPIT.UnixMicro()),
				Temporality: balancehistorystore.TemporalityEffective,
			},
		}, nil)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/volumes?at="+url.QueryEscape(pit), nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAggregateVolumes(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Empty(t, w.Header().Get(historicalBalanceViewHeader))
}

func TestHandleAggregateVolumes_WithGroups(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ *commonpb.QueryFilter, _ query.AggregateOptions, _ appctrl.AggregateVolumesReadOptions) (*appctrl.AggregateVolumesResult, error) {
			return aggregateVolumesResult(&commonpb.AggregateResult{
				Groups: []*commonpb.GroupedAggregateResult{
					{
						Prefix: "users:",
						Volumes: []*commonpb.AggregatedVolume{
							{
								Asset:  "EUR/2",
								Input:  commonpb.NewUint256FromUint64(500),
								Output: commonpb.NewUint256FromUint64(200),
							},
						},
					},
				},
			}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/volumes?groupByPrefixes=users:", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAggregateVolumes(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	wrapper := decodeResponse[BaseResponse[aggregateVolumesResponseJSON]](t, w)
	resp := wrapper.Data
	require.Len(t, resp.Groups, 1)
	require.Equal(t, "users:", resp.Groups[0].Prefix)
	require.Len(t, resp.Groups[0].Volumes, 1)
	require.Equal(t, "EUR/2", resp.Groups[0].Volumes[0].Asset)
	require.Equal(t, "500", resp.Groups[0].Volumes[0].Input)
	require.Equal(t, "200", resp.Groups[0].Volumes[0].Output)
	require.Equal(t, "300", resp.Groups[0].Volumes[0].Balance)
}

func TestHandleAggregateVolumes_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/volumes", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleAggregateVolumes(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAggregateVolumes_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ *commonpb.QueryFilter, _ query.AggregateOptions, _ appctrl.AggregateVolumesReadOptions) (*appctrl.AggregateVolumesResult, error) {
			return nil, errors.New("internal error")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/volumes", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAggregateVolumes(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleAggregateVolumes_LedgerNotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ *commonpb.QueryFilter, _ query.AggregateOptions, _ appctrl.AggregateVolumesReadOptions) (*appctrl.AggregateVolumesResult, error) {
			return nil, &domain.ErrLedgerNotFound{Name: "missing"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/missing/volumes", nil, map[string]string{
		"ledgerName": "missing",
	})

	srv.handleAggregateVolumes(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleAggregateVolumes_NoLeaderError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ *commonpb.QueryFilter, _ query.AggregateOptions, _ appctrl.AggregateVolumesReadOptions) (*appctrl.AggregateVolumesResult, error) {
			return nil, commonpb.ErrNoLeader
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/volumes", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAggregateVolumes(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Equal(t, "1", w.Header().Get("Retry-After"))
}

func TestHandleAggregateVolumes_HistoricalBalanceRetryContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		err            error
		wantStatus     int
		wantErrorCode  string
		wantRetryAfter string
	}{
		{
			name:           "building",
			err:            fmt.Errorf("opening view: %w", &balancehistorystore.ErrBuilding{Current: 3, Target: 5}),
			wantStatus:     http.StatusServiceUnavailable,
			wantErrorCode:  "HISTORY_BUILDING",
			wantRetryAfter: "1",
		},
		{
			name:           "behind",
			err:            fmt.Errorf("opening view: %w", &balancehistorystore.ErrBehind{Current: 3, Required: 5}),
			wantStatus:     http.StatusServiceUnavailable,
			wantErrorCode:  "HISTORY_BEHIND",
			wantRetryAfter: "1",
		},
		{
			name:          "source missing is not retry hinted",
			err:           &balancehistorystore.ErrSourceMissing{Detail: "cold run absent"},
			wantStatus:    http.StatusInternalServerError,
			wantErrorCode: "HISTORY_SOURCE_MISSING",
		},
		{
			name:          "corrupt is not retry hinted",
			err:           &balancehistorystore.ErrCorrupt{Detail: "checksum mismatch"},
			wantStatus:    http.StatusInternalServerError,
			wantErrorCode: "HISTORY_CORRUPT",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, test.err)
			srv := newTestServer(t, backend)

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodGet, "/my-ledger/volumes?at=2026-01-01T00%3A00%3A00Z", nil, map[string]string{
				"ledgerName": "my-ledger",
			})

			srv.handleAggregateVolumes(w, r)

			require.Equal(t, test.wantStatus, w.Code)
			require.Equal(t, test.wantRetryAfter, w.Header().Get("Retry-After"))
			require.Contains(t, w.Body.String(), `"errorCode":"`+test.wantErrorCode+`"`)
		})
	}
}

func TestHandleAggregateVolumes_FullRouteIntegration(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ *commonpb.QueryFilter, _ query.AggregateOptions, _ appctrl.AggregateVolumesReadOptions) (*appctrl.AggregateVolumesResult, error) {
			return aggregateVolumesResult(&commonpb.AggregateResult{
				Volumes: []*commonpb.AggregatedVolume{
					{
						Asset:  "USD/2",
						Input:  commonpb.NewUint256FromUint64(100),
						Output: commonpb.NewUint256FromUint64(50),
					},
				},
			}), nil
		}).AnyTimes()

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v3/my-ledger/volumes", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

// TestHandleAggregateVolumes_EmitsColorAlways pins the wire shape: the
// `color` field is present on every aggregate entry, including for the
// uncolored bucket (empty string). The OpenAPI contract documents
// color as first-class; an `omitempty` tag would drop the field exactly
// when color="" and break clients that expect it on every entry.
func TestHandleAggregateVolumes_EmitsColorAlways(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ *commonpb.QueryFilter, opts query.AggregateOptions, _ appctrl.AggregateVolumesReadOptions) (*appctrl.AggregateVolumesResult, error) {
			require.True(t, opts.CollapseColors, "?collapseColors=true must reach the backend")

			return aggregateVolumesResult(&commonpb.AggregateResult{
				Volumes: []*commonpb.AggregatedVolume{
					{
						Asset:  "USD/2",
						Color:  "", // uncolored / collapsed bucket
						Input:  commonpb.NewUint256FromUint64(100),
						Output: commonpb.NewUint256FromUint64(30),
					},
				},
			}), nil
		})

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v3/my-ledger/volumes?collapseColors=true", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"color":""`,
		`empty color must surface as "color":"" not be omitted by omitempty`)
}

// TestHandleAggregateVolumes_DualFormatFilter is the endpoint-level EN-1511
// acceptance check for volumes: the same logical account selector passed via
// `?filter=` in the textual form and in the structured JSON form reaches the
// backend as the same QueryFilter.
func TestHandleAggregateVolumes_DualFormatFilter(t *testing.T) {
	t.Parallel()

	capture := func(t *testing.T, target string) *commonpb.QueryFilter {
		t.Helper()

		var captured *commonpb.QueryFilter

		backend := NewMockBackend(gomock.NewController(t))
		backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ string, filter *commonpb.QueryFilter, _ query.AggregateOptions, _ appctrl.AggregateVolumesReadOptions) (*appctrl.AggregateVolumesResult, error) {
				captured = filter

				return aggregateVolumesResult(&commonpb.AggregateResult{}), nil
			}).AnyTimes()
		srv := newTestServer(t, backend)

		w := httptest.NewRecorder()
		r := newRequest(t, http.MethodGet, target, nil, map[string]string{"ledgerName": "my-ledger"})
		srv.handleAggregateVolumes(w, r)

		require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
		require.NotNil(t, captured)

		return captured
	}

	fromText := capture(t, "/my-ledger/volumes?filter="+url.QueryEscape(`metadata[status] == "active"`))
	fromJSON := capture(t, "/my-ledger/volumes?filter="+url.QueryEscape(`{"$match":{"metadata[status]":"active"}}`))

	require.True(t, proto.Equal(fromText, fromJSON),
		"textual and JSON ?filter= forms must reach the backend as the same QueryFilter\n text: %v\n json: %v",
		fromText, fromJSON)
}

// TestHandleAggregateVolumes_FilterReachesBackend proves the canonical `filter`
// is the sole account selector: address and metadata conditions both reach the
// controller, and the removed `prefix=` parameter is no longer interpreted (its
// canonical replacement is the textual `address ^= "<prefix>"`).
func TestHandleAggregateVolumes_FilterReachesBackend(t *testing.T) {
	t.Parallel()

	capture := func(t *testing.T, target string) *commonpb.QueryFilter {
		t.Helper()

		var captured *commonpb.QueryFilter

		backend := NewMockBackend(gomock.NewController(t))
		backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ string, filter *commonpb.QueryFilter, _ query.AggregateOptions, _ appctrl.AggregateVolumesReadOptions) (*appctrl.AggregateVolumesResult, error) {
				captured = filter

				return aggregateVolumesResult(&commonpb.AggregateResult{}), nil
			}).AnyTimes()
		srv := newTestServer(t, backend)

		w := httptest.NewRecorder()
		r := newRequest(t, http.MethodGet, target, nil, map[string]string{"ledgerName": "my-ledger"})
		srv.handleAggregateVolumes(w, r)

		require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

		return captured
	}

	// Address-prefix selection reaches the backend as a HardcodedPrefix (the sole
	// filter is not wrapped in a redundant 1-element $and).
	fromAddress := capture(t, "/my-ledger/volumes?filter="+url.QueryEscape(`address ^= "users:"`))
	require.NotNil(t, fromAddress)
	require.Equal(t, "users:", fromAddress.GetAddress().GetHardcodedPrefix())

	// Metadata selection reaches the backend as a field condition.
	fromMetadata := capture(t, "/my-ledger/volumes?filter="+url.QueryEscape(`metadata[status] == "active"`))
	require.NotNil(t, fromMetadata)
	require.NotNil(t, fromMetadata.GetField(), "metadata filter must reach the backend as a field condition")

	// The removed `prefix=` parameter must no longer be interpreted: passed alone
	// (no `filter=`), it yields an unfiltered read (nil filter).
	aliasOnly := capture(t, "/my-ledger/volumes?prefix=users:")
	require.Nil(t, aliasOnly, "the removed prefix= parameter must not build a filter")
}

// TestHandleAggregateVolumes_FilterInvalidForTarget checks that a malformed
// filter and a condition invalid on the Accounts target are both rejected with
// a 400 for both dual-format forms, without invoking the backend (the mock has
// no expectation, so any call fails the test).
func TestHandleAggregateVolumes_FilterInvalidForTarget(t *testing.T) {
	t.Parallel()

	// `ledger == ...` is a logs-only condition, invalid for the Accounts target;
	// `metadata[status ==` is syntactically malformed.
	for _, raw := range []string{
		`ledger == "main"`,
		`{"$match":{"ledger":"main"}}`,
		`metadata[status ==`,
	} {
		srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

		w := httptest.NewRecorder()
		r := newRequest(t, http.MethodGet, "/my-ledger/volumes?filter="+url.QueryEscape(raw), nil,
			map[string]string{"ledgerName": "my-ledger"})
		srv.handleAggregateVolumes(w, r)

		require.Equal(t, http.StatusBadRequest, w.Code, "raw: %s", raw)
	}
}
