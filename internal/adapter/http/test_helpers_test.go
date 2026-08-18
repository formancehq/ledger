package http

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
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

// backendReturningLogs builds a mock backend whose Apply returns the given logs
// and no error, for exercising the unitary-handler log-response contract. Apply
// is expected exactly once, so reaching the log-response path is itself checked.
func backendReturningLogs(t *testing.T, logs []*commonpb.Log) *MockBackend {
	t.Helper()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return logs, nil
		}).Times(1)

	return backend
}

// requirePanicsContaining asserts fn panics with a value whose string rendering
// contains want. Unlike require.Panics, it pins the panic to the intended
// branch: an accidental nil dereference panics with a different value and fails
// the assertion instead of passing for the wrong reason.
func requirePanicsContaining(t *testing.T, want string, fn func()) {
	t.Helper()

	defer func() {
		rec := recover()
		require.NotNil(t, rec, "expected a panic containing %q", want)
		require.Contains(t, fmt.Sprint(rec), want)
	}()

	fn()
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

// aboveJSNumberLimit is 2^53 + 1, the smallest integer a JavaScript double
// cannot represent exactly. The EN-1779 route tests pin it so that a body routed
// through a float would lose its last digit and fail.
const aboveJSNumberLimit = "9007199254740993"

// bigAmountTransaction is the shared EN-1779 route fixture: one posting whose
// amount is aboveJSNumberLimit, between accounts containing `<` and `&`.
//
// Those two characters are load-bearing, not decoration. This package has two
// production JSON writers running on different sonic configurations — ConfigStd
// (HTML-escapes, appends a trailing newline) and ConfigDefault (neither) — and
// `<`/`&` are the only thing that makes a swap between them visible in the
// response body. A fixture without them passes even when a route silently
// changes encoder, which is exactly the regression the opt-in header must not
// introduce.
func bigAmountTransaction(t *testing.T, id uint64) *commonpb.Transaction {
	t.Helper()

	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok, "fixture amount must parse")

	return &commonpb.Transaction{
		Id: id,
		Postings: []*commonpb.Posting{
			commonpb.NewPosting("world<&>", "alice&<bob", "USD/2", amount),
		},
	}
}

// requireOnlyAmountQuotingDiffers asserts that two pinned response bodies are
// identical once the opt-in body's quoted amount is unquoted. Asserting it
// mechanically is what lets the pinned literals catch an encoder swap (EN-1779):
// a change in HTML escaping, framing or field order would otherwise land in both
// literals at once and pass review unnoticed.
func requireOnlyAmountQuotingDiffers(t *testing.T, defaultBody, optInBody string) {
	t.Helper()

	require.Equal(t, defaultBody,
		strings.Replace(optInBody, `"`+aboveJSNumberLimit+`"`, aboveJSNumberLimit, 1),
		"the opt-in wire must differ from the default wire only in the amount's quoting")
}
