package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestHandleAggregateVolumes_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ledgerName string, filter *commonpb.QueryFilter, opts query.AggregateOptions) (*commonpb.AggregateResult, error) {
			require.Equal(t, "my-ledger", ledgerName)
			require.Nil(t, filter)
			require.False(t, opts.UseMaxPrecision)
			require.Empty(t, opts.GroupByPrefixes)

			return &commonpb.AggregateResult{
				Volumes: []*commonpb.AggregatedVolume{
					{
						Asset:  "USD/2",
						Input:  commonpb.NewUint256FromUint64(1000),
						Output: commonpb.NewUint256FromUint64(400),
					},
				},
			}, nil
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
}

func TestHandleAggregateVolumes_WithOptions(t *testing.T) {
	t.Parallel()

	var capturedOpts query.AggregateOptions
	var capturedFilter *commonpb.QueryFilter

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, filter *commonpb.QueryFilter, opts query.AggregateOptions) (*commonpb.AggregateResult, error) {
			capturedOpts = opts
			capturedFilter = filter

			return &commonpb.AggregateResult{}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/volumes?useMaxPrecision=true&groupByPrefixes=users:,merchants:&prefix=users:", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAggregateVolumes(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, capturedOpts.UseMaxPrecision)
	require.Equal(t, []string{"users:", "merchants:"}, capturedOpts.GroupByPrefixes)
	require.NotNil(t, capturedFilter)
	require.Equal(t, "users:", capturedFilter.GetAddress().GetHardcodedPrefix())
}

func TestHandleAggregateVolumes_WithGroups(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ *commonpb.QueryFilter, _ query.AggregateOptions) (*commonpb.AggregateResult, error) {
			return &commonpb.AggregateResult{
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
			}, nil
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
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ *commonpb.QueryFilter, _ query.AggregateOptions) (*commonpb.AggregateResult, error) {
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
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ *commonpb.QueryFilter, _ query.AggregateOptions) (*commonpb.AggregateResult, error) {
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
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ *commonpb.QueryFilter, _ query.AggregateOptions) (*commonpb.AggregateResult, error) {
			return nil, commonpb.ErrNoLeader
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/volumes", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAggregateVolumes(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestHandleAggregateVolumes_FullRouteIntegration(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ *commonpb.QueryFilter, _ query.AggregateOptions) (*commonpb.AggregateResult, error) {
			return &commonpb.AggregateResult{
				Volumes: []*commonpb.AggregatedVolume{
					{
						Asset:  "USD/2",
						Input:  commonpb.NewUint256FromUint64(100),
						Output: commonpb.NewUint256FromUint64(50),
					},
				},
			}, nil
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
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ *commonpb.QueryFilter, opts query.AggregateOptions) (*commonpb.AggregateResult, error) {
			require.True(t, opts.CollapseColors, "?collapseColors=true must reach the backend")

			return &commonpb.AggregateResult{
				Volumes: []*commonpb.AggregatedVolume{
					{
						Asset:  "USD/2",
						Color:  "", // uncolored / collapsed bucket
						Input:  commonpb.NewUint256FromUint64(100),
						Output: commonpb.NewUint256FromUint64(30),
					},
				},
			}, nil
		})

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/my-ledger/volumes?collapseColors=true", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"color":""`,
		`empty color must surface as "color":"" not be omitted by omitempty`)
}
