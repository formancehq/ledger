package contextlogger

import (
	"net/http"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

func WrapRequest(r *http.Request) *http.Request {
	span := trace.SpanFromContext(r.Context())
	contextKeyID := uuid.NewString()
	if span.SpanContext().SpanID().IsValid() {
		contextKeyID = span.SpanContext().SpanID().String()
	}
	return r.WithContext(logging.ContextWithLogger(r.Context(), logging.FromContext(r.Context()).WithFields(map[string]any{
		"contextID": contextKeyID,
	})))
}
