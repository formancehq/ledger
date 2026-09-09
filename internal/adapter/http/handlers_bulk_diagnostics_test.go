package http

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
)

func TestHandleBulkInternalDiagnostics(t *testing.T) {
	t.Parallel()

	for _, atomic := range []bool{false, true} {
		for _, tc := range []struct {
			name    string
			err     error
			cause   string
			code    string
			message string
		}{
			{"unknown", errors.New("pebble: /private/ledger/000123.sst broken"), "pebble: /private/ledger/000123.sst broken", "ERROR", "internal server error (correlation ID: bulk-diagnostics)"},
			{"missing-log", nil, "no log returned from apply", "ERROR", "internal server error (correlation ID: bulk-diagnostics)"},
			{"private-typed", &domain.BusinessError{Err: &domain.ErrIndexInconsistent{Index: "ledger-index", Detail: "/private/index.sst broken"}}, "index ledger-index is inconsistent: /private/index.sst broken", domain.ErrReasonIndexInconsistent, "index is inconsistent"},
			{"numscript", &domain.ErrNumscriptRuntime{Detail: "negative posting amount"}, "numscript runtime error: negative posting amount", domain.ErrReasonNumscriptRuntime, "numscript runtime error: negative posting amount"},
			{"typed-internal", &domain.ErrStorageOperation{Operation: "reading ledger", Cause: errors.New("private")}, "storage operation failed: reading ledger", domain.ErrReasonStorageOperation, "storage operation failed: reading ledger"},
		} {
			if atomic && tc.err == nil {
				continue
			} // The empty-log sentinel belongs to sequential apply.
			t.Run(tc.name+map[bool]string{false: "/sequential", true: "/atomic"}[atomic], func(t *testing.T) {
				t.Parallel()
				for _, continueOnFailure := range []string{"false", "true"} {
					backend := NewMockBackend(gomock.NewController(t))
					backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, tc.err)
					srv := newTestServer(t, backend)
					var logs bytes.Buffer
					recorder := tracetest.NewSpanRecorder()
					provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
					t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
					r := newRequest(t, http.MethodPost, "/ledger1/bulk", strings.NewReader(bulkWriteBody), map[string]string{"ledgerName": "ledger1"})
					ctx, span := provider.Tracer("test").Start(r.Context(), "bulk")
					ctx = logging.ContextWithLogger(ctx, logging.NewDefaultLogger(&logs, false, false, false))
					ctx = context.WithValue(ctx, middleware.RequestIDKey, "bulk-diagnostics")
					url := "/ledger1/bulk?continueOnFailure=" + continueOnFailure
					if atomic {
						url += "&atomic=true"
					}
					r = r.WithContext(ctx)
					r.URL.RawQuery = strings.SplitN(url, "?", 2)[1]
					w := httptest.NewRecorder()
					srv.handleBulk(w, r)
					span.End()
					require.Equal(t, http.StatusInternalServerError, w.Code)
					response := decodeResponse[bulkResponse](t, w)
					require.Len(t, response.Data, 1)
					require.Equal(t, tc.code, response.Data[0].ErrorCode)
					require.Equal(t, tc.message, response.Data[0].ErrorDescription)
					require.Contains(t, logs.String(), tc.cause)
					require.Contains(t, logs.String(), "bulk-diagnostics")
					require.Contains(t, logs.String(), span.SpanContext().TraceID().String())
					require.Equal(t, "bulk-diagnostics", httpSpanAttribute(recorder.Ended()[0], "correlation_id"))
					require.NotEmpty(t, recorder.Ended()[0].Events())
				}
			})
		}
	}
}
