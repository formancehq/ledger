package opentelemetry

import (
	"context"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var Tracer = otel.Tracer("com.formance.ledger")

func Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return Tracer.Start(ctx, name, opts...)
}

func WrapGinContext(ginContext *gin.Context, name string, opts ...trace.SpanStartOption) trace.Span {
	ctx, span := Start(ginContext.Request.Context(), name, opts...)
	ginContext.Request = ginContext.Request.WithContext(ctx)
	return span
}
