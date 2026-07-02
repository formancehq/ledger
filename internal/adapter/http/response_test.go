package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestWriteJSONResponse(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	writeJSONResponse(w, http.StatusOK, map[string]string{"key": "value"})

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	resp := decodeResponse[map[string]string](t, w)
	require.Equal(t, "value", resp["key"])
}

func TestWriteOK(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	writeOK(w, "hello")

	require.Equal(t, http.StatusOK, w.Code)
	resp := decodeResponse[BaseResponse[string]](t, w)
	require.Equal(t, "hello", resp.Data)
}

func TestWriteCreated(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	writeCreated(w, map[string]int{"id": 42})

	require.Equal(t, http.StatusCreated, w.Code)
	resp := decodeResponse[BaseResponse[map[string]int]](t, w)
	require.Equal(t, 42, resp.Data["id"])
}

func TestWriteBadRequest(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	writeBadRequest(w, "VALIDATION", errors.New("invalid input"))

	require.Equal(t, http.StatusBadRequest, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "VALIDATION", resp.ErrorCode)
	require.Equal(t, "invalid input", resp.ErrorMessage)
}

func TestWriteBadRequest_MaxBytesError(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	// Wrap a MaxBytesError so errors.As can unwrap it.
	err := fmt.Errorf("read body: %w", &http.MaxBytesError{Limit: 1048576})
	writeBadRequest(w, "INVALID_REQUEST", err)

	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "BODY_TOO_LARGE", resp.ErrorCode)
}

func TestWriteInternalServerError(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logger := logging.NewDefaultLogger(&logs, false, false, false)

	ctx := logging.ContextWithLogger(context.Background(), logger)
	ctx = context.WithValue(ctx, middleware.RequestIDKey, "corr-123")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	writeInternalServerError(w, r, errors.New("boom: /var/lib/pebble path leaked"))

	require.Equal(t, http.StatusInternalServerError, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "INTERNAL_ERROR", resp.ErrorCode)

	// The client must receive only a generic message carrying the correlation
	// ID — never the raw error text (EN-1442 information disclosure fix).
	require.Equal(t, "internal server error (correlation ID: corr-123)", resp.ErrorMessage)
	require.NotContains(t, resp.ErrorMessage, "boom")
	require.NotContains(t, resp.ErrorMessage, "pebble")

	// The raw error and correlation ID must be logged server-side so ops can
	// correlate the client-reported ID with the real cause.
	logged := logs.String()
	require.Contains(t, logged, "boom: /var/lib/pebble path leaked")
	require.Contains(t, logged, "corr-123")
}

func TestWriteErrorResponse_NilError(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	writeErrorResponse(w, http.StatusBadRequest, "TEST", nil)

	require.Equal(t, http.StatusBadRequest, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "TEST", resp.ErrorCode)
	require.Empty(t, resp.ErrorMessage)
}

func TestWantsHTTPProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		header   string
		expected bool
	}{
		{"with header", "true", true},
		{"with any value", "1", true},
		{"without header", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.header != "" {
				r.Header.Set("X-Query-Profile", tc.header)
			}

			assert.Equal(t, tc.expected, wantsHTTPProfile(r))
		})
	}
}

func TestWriteProfileHeader(t *testing.T) {
	t.Parallel()

	profile := &query.QueryProfile{
		IndexDuration:  5 * time.Millisecond,
		ItemsCollected: 10,
		Root: &query.IteratorStats{
			Label:     "PrefixIterator(exist:ledger:a:)",
			Kind:      "Prefix",
			NextCalls: 15,
		},
	}

	w := httptest.NewRecorder()
	writeProfileHeader(w, profile)

	headerVal := w.Header().Get("X-Query-Profile-Result")
	require.NotEmpty(t, headerVal)

	// Decode and verify
	data, err := base64.StdEncoding.DecodeString(headerVal)
	require.NoError(t, err)

	var pb servicepb.QueryProfile
	require.NoError(t, proto.Unmarshal(data, &pb))

	assert.Equal(t, int64(5000), pb.GetIndexDurationUs())
	assert.Equal(t, int32(10), pb.GetItemsCollected())
	require.NotNil(t, pb.GetRootIterator())
	assert.Equal(t, "Prefix", pb.GetRootIterator().GetKind())
	assert.Equal(t, int64(15), pb.GetRootIterator().GetNextCalls())
}

func TestWriteProfileHeader_NilProfile(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	writeProfileHeader(w, nil)

	assert.Empty(t, w.Header().Get("X-Query-Profile-Result"))
}

func TestQueryParamBool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		query    string
		key      string
		expected bool
	}{
		{"true value", "?flag=true", "flag", true},
		{"false value", "?flag=false", "flag", false},
		{"missing key", "", "flag", false},
		{"empty value", "?flag=", "flag", false},
		{"other value", "?flag=yes", "flag", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, "/"+tc.query, nil)
			require.Equal(t, tc.expected, queryParamBool(r, tc.key))
		})
	}
}
