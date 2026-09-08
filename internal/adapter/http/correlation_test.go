package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestCorrelationID_RejectsUnsafeInboundValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "too long", id: strings.Repeat("a", maxCorrelationIDLength+1)},
		{name: "line break", id: "forged\nlog-entry"},
		{name: "invalid utf8", id: string([]byte{0xff, 0xfe})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.WithValue(context.Background(), middleware.RequestIDKey, tt.id)
			r := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
			got := correlationID(r)

			require.NotEqual(t, tt.id, got)
			require.Len(t, got, 16)
		})
	}
}

func TestCorrelationID_AcceptsMaximumLength(t *testing.T) {
	t.Parallel()

	id := strings.Repeat("a", maxCorrelationIDLength)
	ctx := context.WithValue(context.Background(), middleware.RequestIDKey, id)
	r := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)

	require.Equal(t, id, correlationID(r))
}
