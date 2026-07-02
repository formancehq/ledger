package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/stretchr/testify/require"
)

func TestCorrelationID_ReusesRequestID(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), middleware.RequestIDKey, "req-42")
	r := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)

	require.Equal(t, "req-42", correlationID(r))
}

func TestCorrelationID_FallsBackWhenNoRequestID(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodGet, "/", nil)

	// No RequestID middleware ran: still return a non-empty token so the
	// client always has something to quote.
	require.NotEmpty(t, correlationID(r))
}
