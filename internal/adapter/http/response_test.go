package http

import (
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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

func TestWriteInternalServerError(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	writeInternalServerError(w, r, errors.New("boom"))

	require.Equal(t, http.StatusInternalServerError, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "INTERNAL_ERROR", resp.ErrorCode)
	require.Equal(t, "boom", resp.ErrorMessage)
}

func TestWriteErrorResponse_NilError(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	writeErrorResponse(w, http.StatusBadRequest, "TEST", nil)

	require.Equal(t, http.StatusBadRequest, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "TEST", resp.ErrorCode)
	require.Equal(t, "", resp.ErrorMessage)
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

	assert.Equal(t, int64(5000), pb.IndexDurationUs)
	assert.Equal(t, int32(10), pb.ItemsCollected)
	require.NotNil(t, pb.RootIterator)
	assert.Equal(t, "Prefix", pb.RootIterator.Kind)
	assert.Equal(t, int64(15), pb.RootIterator.NextCalls)
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
