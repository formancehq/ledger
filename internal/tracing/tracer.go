package tracing

import (
	"context"

	"go.opentelemetry.io/otel/attribute"

	"github.com/formancehq/go-libs/time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

//todo: remove global
var Tracer = otel.Tracer("com.formance.ledger")

func Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return Tracer.Start(ctx, name, opts...)
}

func TraceWithLatency[RET any](
	ctx context.Context,
	operationName string,
	fn func(ctx context.Context) (RET, error),
	finalizers ...func(ctx context.Context, ret RET),
) (RET, error) {
	return Trace(ctx, operationName, func(ctx context.Context) (RET, error) {
		now := time.Now()
		ret, err := fn(ctx)
		if err != nil {
			trace.SpanFromContext(ctx).RecordError(err)
			var zeroRet RET
			return zeroRet, err
		}

		latency := time.Since(now)
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("latency", latency.String()))

		for _, finalizer := range finalizers {
			finalizer(ctx, ret)
		}

		return ret, nil
	})
}
