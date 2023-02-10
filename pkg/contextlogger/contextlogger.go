package contextlogger

import (
	"context"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

type contextKey string

var loggerContextKey contextKey = "logger"

type Factory struct {
	underlying logging.LoggerFactory
}

func (c *Factory) Get(ctx context.Context) logging.Logger {
	v := ctx.Value(loggerContextKey)
	if v == nil {
		return c.underlying.Get(ctx)
	}
	return v.(logging.Logger)
}

func NewFactory(underlyingFactory logging.LoggerFactory) *Factory {
	return &Factory{
		underlying: underlyingFactory,
	}
}

var _ logging.LoggerFactory = &Factory{}

func ContextWithLogger(ctx context.Context, logger logging.Logger) context.Context {
	return context.WithValue(ctx, loggerContextKey, logger)
}

func WrapGinRequest(c *gin.Context) {
	span := trace.SpanFromContext(c.Request.Context())
	contextKeyID := uuid.NewString()
	if span.SpanContext().SpanID().IsValid() {
		contextKeyID = span.SpanContext().SpanID().String()
	}
	c.Request = c.Request.WithContext(
		ContextWithLogger(c.Request.Context(), logging.GetLogger(c.Request.Context()).WithFields(map[string]any{
			"contextID": contextKeyID,
		})),
	)
}
