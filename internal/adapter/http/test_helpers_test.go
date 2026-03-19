package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v4/logging"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
)

// newTestServer creates a Server with a mock backend for testing.
func newTestServer(t *testing.T, backend *mockBackend) *Server {
	t.Helper()

	return NewServer(logging.Testing(), backend, 0)
}

// newTestServerWithBulkLimit creates a Server with a mock backend and bulk limit for testing.
func newTestServerWithBulkLimit(t *testing.T, backend *mockBackend, bulkMaxSize int) *Server {
	t.Helper()

	return NewServer(logging.Testing(), backend, bulkMaxSize)
}

// newRequest creates an http.Request with chi URL params.
func newRequest(t *testing.T, method, target string, body io.Reader, chiParams map[string]string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(method, target, body)

	rctx := chi.NewRouteContext()
	for k, v := range chiParams {
		rctx.URLParams.Add(k, v)
	}

	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

// decodeResponse decodes a JSON response body.
func decodeResponse[T any](t *testing.T, w *httptest.ResponseRecorder) T {
	t.Helper()

	var out T

	err := json.Unmarshal(w.Body.Bytes(), &out)
	if err != nil {
		t.Fatalf("failed to decode response: %v (body: %s)", err, w.Body.String())
	}

	return out
}
