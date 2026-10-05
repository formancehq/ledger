package tracing

import (
	"context"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/time"
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
	opts ...TraceOption,
) (RET, error) {
	var zeroRet RET

	return Trace(ctx, tracer, operationName, func(ctx context.Context) (RET, error) {
		now := time.Now()
		ret, err := fn(ctx)
		if err != nil {
			// Error is already recorded by Trace(), no need to record it here
			return zeroRet, err
		}

		latency := time.Since(now)
		histogram.Record(ctx, latency.Milliseconds())
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("latency", latency.String()))

		return ret, nil
	}, opts...)
}

type traceSettings struct {
	spanStartOptions   []trace.SpanStartOption
	skipRecordErrorIfs []func(error) bool
}

func (s *traceSettings) shouldSkipErrorRecording(err error) bool {
	for _, predicate := range s.skipRecordErrorIfs {
		if predicate(err) {
			return true
		}
	}
	return false
}

// TraceOption configures Trace and TraceWithMetric.
type TraceOption interface {
	applyTrace(*traceSettings)
}

type traceOptionFunc func(*traceSettings)

func (f traceOptionFunc) applyTrace(s *traceSettings) { f(s) }

// WithSpanStartOptions passes options to tracer.Start (for example trace.WithAttributes).
func WithSpanStartOptions(opts ...trace.SpanStartOption) TraceOption {
	return traceOptionFunc(func(s *traceSettings) {
		s.spanStartOptions = append(s.spanStartOptions, opts...)
	})
}

// SkipErrorRecordingIf skips otlp.RecordError when predicate returns true for the returned error.
// The error is still returned to the caller unchanged. Multiple options compose with OR semantics.
func SkipErrorRecordingIf(predicate func(error) bool) TraceOption {
	return traceOptionFunc(func(s *traceSettings) {
		s.skipRecordErrorIfs = append(s.skipRecordErrorIfs, predicate)
	})
}

func Trace[RET any](
	ctx context.Context,
	tracer trace.Tracer,
	name string,
	fn func(ctx context.Context) (RET, error),
	opts ...TraceOption,
) (RET, error) {
	var settings traceSettings
	for _, o := range opts {
		o.applyTrace(&settings)
	}

	ctx, span := tracer.Start(ctx, name, settings.spanStartOptions...)
	defer span.End()

	ret, err := fn(ctx)
	if err != nil {
		if !settings.shouldSkipErrorRecording(err) {
			otlp.RecordError(ctx, err)
		}
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
