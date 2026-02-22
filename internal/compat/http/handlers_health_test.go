package http

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleHealth_Healthy(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{healthy: true}
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

	backend := &mockBackend{healthy: false}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/health", nil, nil)

	srv.handleHealth(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "UNHEALTHY", resp.ErrorCode)
}
