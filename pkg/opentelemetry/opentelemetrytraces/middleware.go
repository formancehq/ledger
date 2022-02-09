package opentelemetrytraces

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/controllers"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func Middleware() func(context *gin.Context) {
	return func(context *gin.Context) {
		defer func() {
			span := trace.SpanFromContext(context.Request.Context())
			for _, e := range context.Errors {
				span.RecordError(e)
			}
			if code := controllers.ErrorCode(context); code != "" {
				span.SetAttributes(attribute.KeyValue{
					Key:   "http.error_code",
					Value: attribute.StringValue(code),
				})
			}
		}()
		context.Next()
	}
}
