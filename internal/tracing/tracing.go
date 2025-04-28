package tracing

import (
	"context"
	"github.com/formancehq/go-libs/v2/otlp"
	"github.com/formancehq/go-libs/v2/time"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"go.opentelemetry.io/otel/trace"
)

func TraceWithMetric[RET any](
	ctx context.Context,
	operationName string,
	tracer trace.Tracer,
	histogram metric.Int64Histogram,
	fn func(ctx context.Context) (RET, error),
	finalizers ...func(ctx context.Context, ret RET),
) (RET, error) {
	var zeroRet RET

	return Trace(ctx, tracer, operationName, func(ctx context.Context) (RET, error) {
		now := time.Now()
		ret, err := fn(ctx)
		if err != nil {
			otlp.RecordError(ctx, err)
			return zeroRet, err
		}

		latency := time.Since(now)
		histogram.Record(ctx, latency.Milliseconds())
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("latency", latency.String()))

		for _, finalizer := range finalizers {
			finalizer(ctx, ret)
		}

		return ret, nil
	})
}

func Trace[RET any](
	ctx context.Context,
	tracer trace.Tracer,
	name string,
	fn func(ctx context.Context) (RET, error),
	spanOptions ...trace.SpanStartOption,
) (RET, error) {
	ctx, span := tracer.Start(ctx, name, spanOptions...)
	defer span.End()

	ret, err := fn(ctx)
	if err != nil {
		otlp.RecordError(ctx, err)
		return ret, err
	}

	return ret, nil
}

func NoResult(fn func(ctx context.Context) error) func(ctx context.Context) (any, error) {
	return func(ctx context.Context) (any, error) {
		return nil, fn(ctx)
	}
}

func SkipResult[RET any](_ RET, err error) error {
	return err
}
