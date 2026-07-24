package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// newTestServer creates a Server with a mock backend for testing.
func newTestServer(t *testing.T, backend Backend) *Server {
	t.Helper()

	return NewServer(logging.Testing(), backend, internalauth.AuthConfig{}, 0)
}

// newTestServerWithBulkLimit creates a Server with a mock backend and bulk limit for testing.
func newTestServerWithBulkLimit(t *testing.T, backend Backend, bulkMaxSize int) *Server {
	t.Helper()

	return NewServer(logging.Testing(), backend, internalauth.AuthConfig{}, bulkMaxSize)
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

// backendReturningLogs builds a mock backend whose Apply returns the given
// logs and no error, for exercising the unitary-handler log-response contract.
func backendReturningLogs(t *testing.T, logs []*commonpb.Log) *MockBackend {
	t.Helper()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return logs, nil
		}).AnyTimes()

	return backend
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
