package http

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestHandleHealth_Healthy(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().IsHealthy().Return(true).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/health", nil, nil)

	srv.handleHealth(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	resp := decodeResponse[BaseResponse[HealthData]](t, w)
	require.Equal(t, "ok", resp.Data.Status)
}

func TestHandleHealth_Unhealthy(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().IsHealthy().Return(false).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/health", nil, nil)

	srv.handleHealth(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "UNHEALTHY", resp.ErrorCode)
}

func TestHandleLivez(t *testing.T) {
	t.Parallel()

	// Livez always returns 200 regardless of backend state.
	backend := NewMockBackend(gomock.NewController(t))
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/livez", nil, nil)

	srv.handleLivez(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	resp := decodeResponse[BaseResponse[HealthData]](t, w)
	require.Equal(t, "ok", resp.Data.Status)
}

func TestHandleReadyz_Ready(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().NotReadyReasons().Return(nil).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/readyz", nil, nil)

	srv.handleReadyz(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	resp := decodeResponse[BaseResponse[HealthData]](t, w)
	require.Equal(t, "ok", resp.Data.Status)
}

func TestHandleReadyz_NotReady(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().NotReadyReasons().Return([]string{"raft loop has not started"}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/readyz", nil, nil)

	srv.handleReadyz(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "NOT_READY", resp.ErrorCode)
}

func TestHandleClusterz_Ready(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().NotClusterReadyReasons().Return(nil).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/clusterz", nil, nil)

	srv.handleClusterz(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	resp := decodeResponse[BaseResponse[HealthData]](t, w)
	require.Equal(t, "ok", resp.Data.Status)
}

func TestHandleClusterz_NotReady(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().NotClusterReadyReasons().Return([]string{"no leader elected"}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/clusterz", nil, nil)

	srv.handleClusterz(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "CLUSTER_NOT_READY", resp.ErrorCode)
}
