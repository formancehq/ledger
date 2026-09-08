package http

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestKindResourceExhaustedMapsTo429(t *testing.T) {
	t.Parallel()
	require.Equal(t, http.StatusTooManyRequests, kindToHTTPStatus(domain.KindResourceExhausted))
}

func TestHandleError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		err            error
		expectedStatus int
		expectedCode   string
		checkRetry     bool
	}{
		{
			name:           "no leader",
			err:            commonpb.ErrNoLeader,
			expectedStatus: http.StatusServiceUnavailable,
			expectedCode:   "NO_LEADER",
			checkRetry:     true,
		},
		{
			name:           "cache horizon exceeded — infrastructure rejection, retryable",
			err:            plan.ErrCacheHorizonExceeded,
			expectedStatus: http.StatusServiceUnavailable,
			expectedCode:   "CACHE_HORIZON_EXCEEDED",
			checkRetry:     true,
		},
		{
			name:           "cache horizon exceeded wrapped by admission",
			err:            fmt.Errorf("building preloads: %w", plan.ErrCacheHorizonExceeded),
			expectedStatus: http.StatusServiceUnavailable,
			expectedCode:   "CACHE_HORIZON_EXCEEDED",
			checkRetry:     true,
		},
		{
			name:           "not found error",
			err:            commonpb.NewNotFoundError("item %d", 1),
			expectedStatus: http.StatusNotFound,
			expectedCode:   "NOT_FOUND",
		},
		{
			// Bare domain.ErrNotFound sentinel — e.g. a read against a deleted
			// ledger (query.GetLedgerByName). Must be a 404, not a 500.
			name:           "bare domain.ErrNotFound sentinel",
			err:            domain.ErrNotFound,
			expectedStatus: http.StatusNotFound,
			expectedCode:   "NOT_FOUND",
		},
		{
			name:           "wrapped domain.ErrNotFound",
			err:            fmt.Errorf("reading ledger info: %w", domain.ErrNotFound),
			expectedStatus: http.StatusNotFound,
			expectedCode:   "NOT_FOUND",
		},
		{
			name:           "ledger already exists",
			err:            &domain.ErrLedgerAlreadyExists{Name: "test"},
			expectedStatus: http.StatusConflict,
			expectedCode:   "LEDGER_ALREADY_EXISTS",
		},
		{
			name:           "ledger not found",
			err:            &domain.ErrLedgerNotFound{Name: "test"},
			expectedStatus: http.StatusNotFound,
			expectedCode:   "LEDGER_NOT_FOUND",
		},
		{
			name:           "transaction reference conflict",
			err:            &domain.ErrTransactionReferenceConflict{Reference: "ref1"},
			expectedStatus: http.StatusConflict,
			expectedCode:   "TRANSACTION_REFERENCE_CONFLICT",
		},
		{
			name:           "idempotency key conflict",
			err:            &domain.ErrIdempotencyKeyConflict{Key: "key1"},
			expectedStatus: http.StatusConflict,
			expectedCode:   "IDEMPOTENCY_KEY_CONFLICT",
		},
		{
			name:           "transaction not found",
			err:            &domain.ErrTransactionNotFound{TransactionID: 42},
			expectedStatus: http.StatusNotFound,
			expectedCode:   "TRANSACTION_NOT_FOUND",
		},
		{
			name:           "transaction already reverted",
			err:            &domain.ErrTransactionAlreadyReverted{TransactionID: 42},
			expectedStatus: http.StatusConflict,
			expectedCode:   "TRANSACTION_ALREADY_REVERTED",
		},
		{
			name:           "insufficient funds",
			err:            &domain.ErrInsufficientFunds{Account: "a", Asset: "USD", Amount: "100", Balance: "50"},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "INSUFFICIENT_FUNDS",
		},
		{
			name:           "sequence exhausted",
			err:            &domain.ErrSequenceExhausted{Counter: domain.SequenceCounterLog},
			expectedStatus: http.StatusTooManyRequests,
			expectedCode:   domain.ErrReasonSequenceExhausted,
		},
		{
			name:           "numscript parse error",
			err:            &domain.ErrNumscriptParse{Details: "syntax error"},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "NUMSCRIPT_PARSE_ERROR",
		},
		{
			name:           "numscript dependency discovery failed",
			err:            &domain.ErrDependencyDiscoveryFailed{Cause: errors.New("non-deterministic script")},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "VALIDATION",
		},
		{
			name:           "metadata not found",
			err:            &domain.ErrMetadataNotFound{Target: "account:foo", Key: "bar"},
			expectedStatus: http.StatusNotFound,
			expectedCode:   "METADATA_NOT_FOUND",
		},
		{
			name:           "target required",
			err:            domain.ErrTargetRequired,
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "VALIDATION",
		},
		{
			name:           "metadata key required",
			err:            domain.ErrMetadataKeyRequired,
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "VALIDATION",
		},
		{
			name:           "script required",
			err:            domain.ErrScriptRequired,
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "VALIDATION",
		},
		{
			name:           "unknown error",
			err:            errors.New("something unexpected"),
			expectedStatus: http.StatusInternalServerError,
			expectedCode:   "INTERNAL_ERROR",
		},
		{
			name:           "grpc unavailable status",
			err:            status.Error(codes.Unavailable, "forwarded stream failed mid-transfer: serving node torn down"),
			expectedStatus: http.StatusServiceUnavailable,
			expectedCode:   "UNAVAILABLE",
			checkRetry:     true,
		},
		{
			name:           "grpc canceled status stays a sanitized 500",
			err:            status.Error(codes.Canceled, "context canceled"),
			expectedStatus: http.StatusInternalServerError,
			expectedCode:   "INTERNAL_ERROR",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/", nil)

			handleError(w, r, tc.err)

			require.Equal(t, tc.expectedStatus, w.Code)

			resp := decodeResponse[ErrorResponse](t, w)
			require.Equal(t, tc.expectedCode, resp.ErrorCode)
			require.NotEmpty(t, resp.ErrorMessage)

			// The fallthrough branch (non-domain errors) must be sanitized:
			// the raw error text must never reach the client, only a generic
			// message with a correlation ID (EN-1442).
			if tc.expectedCode == "INTERNAL_ERROR" {
				require.NotContains(t, resp.ErrorMessage, "something unexpected")
				require.Contains(t, resp.ErrorMessage, "correlation ID")
			}

			if tc.checkRetry {
				require.Equal(t, "1", w.Header().Get("Retry-After"))
			}
		})
	}
}

func TestHandleErrorKindInternalRecordsCorrelatedDiagnostics(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logger := logging.NewDefaultLogger(&logs, false, false, false)
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	ctx, span := provider.Tracer("test").Start(context.Background(), "request")
	ctx = logging.ContextWithLogger(ctx, logger)
	ctx = context.WithValue(ctx, middleware.RequestIDKey, "corr-internal")
	err := &domain.ErrInvalidExecutionPlan{Reason_: "secret structural detail"}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	handleError(w, r, err)
	span.End()

	require.Equal(t, http.StatusInternalServerError, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, domain.ErrReasonInvalidExecutionPlan, resp.ErrorCode)
	require.Equal(t, err.Error(), resp.ErrorMessage, "KindInternal response contracts stay unchanged")
	assert.Contains(t, logs.String(), err.Error())
	assert.Contains(t, logs.String(), "corr-internal")
	assert.Contains(t, logs.String(), span.SpanContext().TraceID().String())
	assert.Contains(t, logs.String(), span.SpanContext().SpanID().String())

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, "corr-internal", httpSpanAttribute(ended[0], "correlation_id"))
	assert.NotEmpty(t, ended[0].Events())
}

func TestHandleErrorSequenceExhaustedDoesNotEnterInternalDiagnostics(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	ctx := logging.ContextWithLogger(context.Background(), logging.NewDefaultLogger(&logs, false, false, false))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)

	handleError(w, r, &domain.ErrSequenceExhausted{Counter: domain.SequenceCounterLog})

	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.Equal(t, domain.ErrReasonSequenceExhausted, decodeResponse[ErrorResponse](t, w).ErrorCode)
	require.Empty(t, logs.String())
}
