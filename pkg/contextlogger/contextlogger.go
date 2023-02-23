package contextlogger

import (
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

func WrapGinRequest(c *gin.Context) {
	span := trace.SpanFromContext(c.Request.Context())
	contextKeyID := uuid.NewString()
	if span.SpanContext().SpanID().IsValid() {
		contextKeyID = span.SpanContext().SpanID().String()
	}
	c.Request = c.Request.WithContext(logging.ContextWithLogger(c.Request.Context(), logging.FromContext(c.Request.Context()).WithFields(map[string]any{
		"contextID": contextKeyID,
	})))
}
