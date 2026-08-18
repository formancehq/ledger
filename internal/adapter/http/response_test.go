package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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

// failingMarshaler is a payload whose MarshalJSON always fails. It is the only
// way to reach the error path of the buffered writers below, and no other
// fixture in this package provides one. The error text is a sentinel so the
// tests can assert it never reaches the client.
type failingMarshaler struct{}

// MarshalJSON implements json.Marshaler, always failing.
func (failingMarshaler) MarshalJSON() ([]byte, error) {
	return nil, errors.New("sentinel-payload-detail")
}

// checkedWriter describes one of this package's buffered response writers. All
// of them marshal the body to a buffer BEFORE writing any header; they differ in
// the sonic configuration they marshal with, and that difference is part of
// every route's wire contract, so the table keeps their expectations separate
// rather than sharing one (EN-1779).
type checkedWriter struct {
	name string
	// write invokes the writer under test, closing over its status code.
	write func(w http.ResponseWriter, r *http.Request, data any)
	// wantStatus is the status the writer emits on success. The error-path
	// subtest asserts the response is NOT this, which is what catches a writer
	// that commits its header before marshaling.
	wantStatus int
	// stream is the pre-existing streaming writer this one must stay
	// byte-identical to, or nil when it has no counterpart on the same encoder
	// (writeOKChecked runs on ConfigDefault, which no streaming writer uses).
	stream func(w http.ResponseWriter, data any)
	// wantHTMLEscaped and wantTrailingNewline pin the sonic configuration:
	// ConfigStd escapes `<`/`&` and appends a trailing newline, ConfigDefault
	// does neither. Asserting them is what makes a swapped encoder fail here
	// instead of silently changing every route's default wire.
	wantHTMLEscaped     bool
	wantTrailingNewline bool
}

// TestWriteCheckedStatus covers every buffered writer — writeCheckedBody,
// writeCheckedStatus and writeOKChecked, whose error path was equally untested —
// as one table.
//
// It exists because two mutations of writeCheckedStatus previously survived the
// whole package suite: hoisting w.WriteHeader above the marshal call (which
// reduces the writer to the very behaviour it was created to prevent, a
// committed success status carrying a failure payload), and deleting the
// Content-Type header (after which a real net/http server sniffs the body as
// text/plain). The error-path and byte-identity subtests below kill both.
//
// writeCheckedBody is listed separately from writeCheckedStatus even though the
// latter now delegates to it: the two differ in whether the payload is wrapped in
// the BaseResponse envelope, and each one's byte-identity reference is the
// streaming writer the routes on it used before — writeJSONResponse for the
// no-envelope form, writeOK/writeCreated for the enveloped one.
func TestWriteCheckedStatus(t *testing.T) {
	t.Parallel()

	writers := []checkedWriter{
		{
			name: "writeOKChecked",
			write: func(w http.ResponseWriter, r *http.Request, data any) {
				writeOKChecked(w, r, data)
			},
			wantStatus:          http.StatusOK,
			stream:              nil,
			wantHTMLEscaped:     false,
			wantTrailingNewline: false,
		},
		{
			name: "writeCheckedBody 200",
			write: func(w http.ResponseWriter, r *http.Request, data any) {
				writeCheckedBody(w, r, http.StatusOK, data)
			},
			wantStatus: http.StatusOK,
			stream: func(w http.ResponseWriter, data any) {
				writeJSONResponse(w, http.StatusOK, data)
			},
			wantHTMLEscaped:     true,
			wantTrailingNewline: true,
		},
		{
			name: "writeCheckedBody 409",
			write: func(w http.ResponseWriter, r *http.Request, data any) {
				writeCheckedBody(w, r, http.StatusConflict, data)
			},
			wantStatus: http.StatusConflict,
			stream: func(w http.ResponseWriter, data any) {
				writeJSONResponse(w, http.StatusConflict, data)
			},
			wantHTMLEscaped:     true,
			wantTrailingNewline: true,
		},
		{
			name: "writeCheckedStatus 200",
			write: func(w http.ResponseWriter, r *http.Request, data any) {
				writeCheckedStatus(w, r, http.StatusOK, data)
			},
			wantStatus:          http.StatusOK,
			stream:              writeOK,
			wantHTMLEscaped:     true,
			wantTrailingNewline: true,
		},
		{
			name: "writeCheckedStatus 201",
			write: func(w http.ResponseWriter, r *http.Request, data any) {
				writeCheckedStatus(w, r, http.StatusCreated, data)
			},
			wantStatus:          http.StatusCreated,
			stream:              writeCreated,
			wantHTMLEscaped:     true,
			wantTrailingNewline: true,
		},
	}

	type payloadCase struct {
		name string
		data any
		// htmlSensitive marks payloads carrying `<`/`&`, the only characters
		// that make the two encoders distinguishable in the body.
		htmlSensitive bool
	}

	payloads := []payloadCase{
		{name: "nil data", data: nil},
		{name: "string with HTML-sensitive characters", data: "a<b&c>d", htmlSensitive: true},
		{name: "map with HTML-sensitive characters", data: map[string]string{"account": "world<&>"}, htmlSensitive: true},
		{name: "typed nil pointer", data: (*commonpb.Transaction)(nil)},
		{name: "populated transaction", data: bigAmountTransaction(t, 7), htmlSensitive: true},
		{
			name:          "detail-route envelope",
			data:          getTransactionData{Transaction: bigAmountTransaction(t, 7), Receipt: "receipt"},
			htmlSensitive: true,
		},
		{
			name:          "string-amount wrapper",
			data:          commonpb.StringAmountTransaction{Transaction: bigAmountTransaction(t, 7)},
			htmlSensitive: true,
		},
		{name: "wrapper over a nil inner pointer", data: commonpb.StringAmountTransaction{Transaction: nil}},
	}

	for _, wr := range writers {
		t.Run(wr.name, func(t *testing.T) {
			t.Parallel()

			t.Run("marshal failure yields a sanitized 500", func(t *testing.T) {
				t.Parallel()

				w := httptest.NewRecorder()
				r := httptest.NewRequest(http.MethodPost, "/", nil)

				wr.write(w, r, failingMarshaler{})

				// The status must NOT have been committed before the marshal:
				// a writer that writes its header first turns a marshal failure
				// into a success status carrying an error body.
				require.Equal(t, http.StatusInternalServerError, w.Code)
				require.NotEqual(t, wr.wantStatus, w.Code)
				require.NotContains(t, w.Body.String(), `"data"`,
					"a failed marshal must not emit any part of the success envelope")

				resp := decodeResponse[ErrorResponse](t, w)
				require.Equal(t, "INTERNAL_ERROR", resp.ErrorCode)

				// Routed through handleError, so the message is the sanitized
				// correlation-ID form (EN-1442). The streaming writers instead
				// append the raw marshal error to an already-committed body,
				// which is what this writer exists to avoid.
				require.Contains(t, resp.ErrorMessage, "correlation ID")
				require.NotContains(t, w.Body.String(), "sentinel-payload-detail")
			})

			for _, pc := range payloads {
				t.Run(pc.name, func(t *testing.T) {
					t.Parallel()

					w := httptest.NewRecorder()
					wr.write(w, httptest.NewRequest(http.MethodGet, "/", nil), pc.data)

					require.Equal(t, wr.wantStatus, w.Code)
					require.Equal(t, "application/json", w.Header().Get("Content-Type"),
						"a missing Content-Type makes a real server sniff the body as text/plain")

					body := w.Body.String()
					require.Equal(t, wr.wantTrailingNewline, strings.HasSuffix(body, "\n"),
						"trailing newline pins the sonic configuration: body %q", body)

					if pc.htmlSensitive {
						if wr.wantHTMLEscaped {
							require.Contains(t, body, `\u003c`)
							require.NotContains(t, body, "<")
						} else {
							require.Contains(t, body, "<")
							require.NotContains(t, body, `\u003c`)
						}
					}

					if wr.stream == nil {
						return
					}

					// Byte-identity with the streaming writer the routes used
					// before, headers included: adding the buffered marshal must
					// not move a single byte for clients that never opt in.
					ref := httptest.NewRecorder()
					wr.stream(ref, pc.data)

					require.Equal(t, ref.Body.String(), body)
					require.Equal(t, ref.Code, w.Code)
					require.Equal(t, ref.Header(), w.Header())
				})
			}
		})
	}
}

// TestWriteCheckedBody_DoesNotWrapInBaseResponse pins the one behavioural
// difference between this package's two ConfigStd buffered writers. Bulk and
// prepared-query build their own top-level envelope, so a writeCheckedBody that
// wrapped its argument the way writeCheckedStatus does would double-wrap them
// into {"data":{"data":…}} — which is why the extraction kept two entry points
// instead of routing every caller through the enveloping one (EN-1779).
func TestWriteCheckedBody_DoesNotWrapInBaseResponse(t *testing.T) {
	t.Parallel()

	payload := bulkResponse{Data: []bulkAPIResult{{ResponseType: "CREATE_TRANSACTION", LogID: 17}}}

	bare := httptest.NewRecorder()
	writeCheckedBody(bare, httptest.NewRequest(http.MethodPost, "/", nil), http.StatusOK, payload)

	enveloped := httptest.NewRecorder()
	writeCheckedStatus(enveloped, httptest.NewRequest(http.MethodPost, "/", nil), http.StatusOK, payload)

	require.Equal(t, `{"data":[{"responseType":"CREATE_TRANSACTION","logID":17}]}`+"\n", bare.Body.String())
	require.Equal(t, `{"data":{"data":[{"responseType":"CREATE_TRANSACTION","logID":17}]}}`+"\n", enveloped.Body.String())
}
