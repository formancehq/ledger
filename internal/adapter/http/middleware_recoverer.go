package http

import (
	"fmt"
	"net/http"
	"runtime/debug"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// jsonRecoverer is a middleware that recovers from panics and returns a JSON
// error response instead of the default text/plain from Chi's Recoverer.
// This ensures all responses (including panic recovery) use application/json
// content type for OpenAPI conformance.
//
// The raw panic value and stack are logged server-side and recorded on the
// OTel span with a correlation ID; the client only receives a generic message
// carrying that same ID. The panic value can embed internal file paths,
// invariant strings, and goroutine state, so it must never reach the client
// (mirrors the gRPC adapter's handlePanic, #375).
func jsonRecoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rvr := recover(); rvr != nil {
				if rvr == http.ErrAbortHandler { //nolint:errorlint // rvr is interface{}, not error
					// Respect ErrAbortHandler - don't recover, let the server handle it.
					panic(rvr)
				}

				id := correlationID(r)
				stack := debug.Stack()

				logging.FromContext(r.Context()).WithFields(map[string]any{
					"correlation_id": id,
				}).Errorf("HTTP handler panicked: %v\n%s", rvr, stack)

				if span := trace.SpanFromContext(r.Context()); span.SpanContext().IsValid() {
					span.SetAttributes(
						attribute.String("panic.value", fmt.Sprintf("%v", rvr)),
						attribute.String("panic.stack", string(stack)),
						attribute.String("correlation_id", id),
					)
					span.RecordError(fmt.Errorf("panic recovered (correlation ID: %s)", id))
				}

				writeErrorResponse(
					w,
					http.StatusInternalServerError,
					"INTERNAL_ERROR",
					fmt.Errorf("internal server error (correlation ID: %s)", id),
				)
			}
		}()

		next.ServeHTTP(w, r)
	})
}
