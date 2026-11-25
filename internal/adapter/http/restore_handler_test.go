package http

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalgrpc "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

func serveRestore(t *testing.T, backend RestoreBackend, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	NewRestoreHandler(logging.Testing(), backend).ServeHTTP(w, r)

	return w
}

// ndjsonLines decodes every line of an NDJSON body.
func ndjsonLines(t *testing.T, w *httptest.ResponseRecorder) []map[string]any {
	t.Helper()
	var lines []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(w.Body.String()))
	for scanner.Scan() {
		var line map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &line), scanner.Text())
		lines = append(lines, line)
	}
	require.NoError(t, scanner.Err())

	return lines
}

func TestRestoreHandler_Health(t *testing.T) {
	t.Parallel()
	for name, handler := range map[string]http.Handler{
		"api":         NewRestoreHandler(logging.Testing(), NewMockRestoreBackend(gomock.NewController(t))),
		"health only": NewRestoreHealthHandler(logging.Testing()),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
			require.Equal(t, http.StatusOK, w.Code)
			require.JSONEq(t, `{"status":"restore_mode"}`, w.Body.String())
		})
	}
}

func TestRestoreHealthHandler_NoAPI(t *testing.T) {
	t.Parallel()
	handler := NewRestoreHealthHandler(logging.Testing())
	for _, route := range []struct{ method, target string }{
		{http.MethodPost, "/v3/_/restore/downloads"},
		{http.MethodGet, "/v3/_/restore/downloads/job"},
		{http.MethodDelete, "/v3/_/restore/downloads/job"},
		{http.MethodPost, "/v3/_/restore/validate"},
		{http.MethodGet, "/v3/_/restore/preview"},
		{http.MethodPost, "/v3/_/restore/finalize"},
	} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(route.method, route.target, nil))
		require.Equal(t, http.StatusNotFound, w.Code, "%s %s", route.method, route.target)
	}
}

// TestRestoreHandler_Lifecycle drives the real RestoreService: stages that
// need a downloaded backup are refused, and once shutdown begins every route
// answers 503.
func TestRestoreHandler_Lifecycle(t *testing.T) {
	t.Parallel()
	impl := internalgrpc.NewRestoreServiceServer(t.TempDir(), "cluster", 1, logging.Testing())
	t.Cleanup(func() {
		impl.BeginShutdown()
		impl.Shutdown()
	})

	for _, route := range []struct{ method, target string }{
		{http.MethodPost, "/v3/_/restore/validate"},
		{http.MethodGet, "/v3/_/restore/preview"},
		{http.MethodPost, "/v3/_/restore/finalize"},
	} {
		w := serveRestore(t, impl, route.method, route.target, "")
		require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		resp := decodeErrorResponse(t, w)
		require.Equal(t, "FAILED_PRECONDITION", resp.ErrorCode)
		require.Equal(t, "no backup downloaded; download a backup first", resp.ErrorMessage)
	}

	w := serveRestore(t, impl, http.MethodGet, "/v3/_/restore/downloads/unknown", "")
	require.Equal(t, http.StatusNotFound, w.Code, w.Body.String())

	impl.BeginShutdown()
	w = serveRestore(t, impl, http.MethodGet, "/v3/_/restore/preview", "")
	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	require.Equal(t, "1", w.Header().Get("Retry-After"))
}

var _ RestoreBackend = (*internalgrpc.RestoreServiceServerImpl)(nil)

func TestValidateRestoreEvent_JSON(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		event *restorepb.ValidateRestoreEvent
		want  string
	}{
		{
			event: &restorepb.ValidateRestoreEvent{Type: &restorepb.ValidateRestoreEvent_Progress{Progress: &restorepb.ValidateRestoreProgress{LogsChecked: 1, TotalLogs: 2}}},
			want:  `{"progress":{"logsChecked":1,"totalLogs":2}}`,
		},
		{
			event: &restorepb.ValidateRestoreEvent{Type: &restorepb.ValidateRestoreEvent_Error{Error: &restorepb.ValidateRestoreError{Message: "bad"}}},
			want:  `{"error":{"message":"bad"}}`,
		},
	} {
		got, err := json.Marshal(tc.event)
		require.NoError(t, err)
		require.JSONEq(t, tc.want, string(got))
	}
}
