package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestJSONRecoverer_SanitizesPanic(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logger := logging.NewDefaultLogger(&logs, false, false, false)

	panicking := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("secret invariant: /var/lib/ledger/pebble corrupted")
	})

	ctx := logging.ContextWithLogger(context.Background(), logger)
	ctx = context.WithValue(ctx, middleware.RequestIDKey, "corr-panic")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)

	jsonRecoverer(panicking).ServeHTTP(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))

	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "INTERNAL_ERROR", resp.ErrorCode)

	// The client must never see the raw panic value.
	require.Equal(t, "internal server error (correlation ID: corr-panic)", resp.ErrorMessage)
	require.NotContains(t, resp.ErrorMessage, "secret invariant")
	require.NotContains(t, resp.ErrorMessage, "pebble")

	// The raw panic value, the stack, and the correlation ID are logged
	// server-side so ops can resolve the client-reported ID.
	logged := logs.String()
	require.Contains(t, logged, "secret invariant: /var/lib/ledger/pebble corrupted")
	require.Contains(t, logged, "corr-panic")
	require.Contains(t, logged, "HTTP handler panicked")
}

func TestJSONRecoverer_PropagatesErrAbortHandler(t *testing.T) {
	t.Parallel()

	aborting := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)

	// ErrAbortHandler must be re-panicked so the server can handle it, not
	// swallowed into a JSON 500.
	require.PanicsWithValue(t, http.ErrAbortHandler, func() {
		jsonRecoverer(aborting).ServeHTTP(w, r)
	})
}

func TestJSONRecoverer_NoPanicPassesThrough(t *testing.T) {
	t.Parallel()

	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)

	jsonRecoverer(ok).ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"ok":true}`, w.Body.String())
}
