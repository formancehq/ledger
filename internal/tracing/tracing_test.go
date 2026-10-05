package tracing

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

var (
	errExpected   = errors.New("expected")
	errUnexpected = errors.New("unexpected")
)

func isExpected(err error) bool { return errors.Is(err, errExpected) }

func runTrace(t *testing.T, fnErr error, opts ...TraceOption) sdktrace.ReadOnlySpan {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)).Tracer("test")

	_, err := Trace(context.Background(), tracer, "op", func(ctx context.Context) (any, error) {
		return nil, fnErr
	}, opts...)
	require.ErrorIs(t, err, fnErr)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	return spans[0]
}

func TestTraceSkipErrorRecordingIf(t *testing.T) {
	t.Parallel()

	t.Run("matching error is returned but not recorded", func(t *testing.T) {
		t.Parallel()
		span := runTrace(t, errExpected, SkipErrorRecordingIf(isExpected))
		require.Equal(t, codes.Unset, span.Status().Code)
		require.Empty(t, span.Events())
	})

	t.Run("wrapped matching error is not recorded", func(t *testing.T) {
		t.Parallel()
		span := runTrace(t, errors.Join(errors.New("context"), errExpected), SkipErrorRecordingIf(isExpected))
		require.Equal(t, codes.Unset, span.Status().Code)
		require.Empty(t, span.Events())
	})

	t.Run("non-matching error is recorded", func(t *testing.T) {
		t.Parallel()
		span := runTrace(t, errUnexpected, SkipErrorRecordingIf(isExpected))
		require.Equal(t, codes.Error, span.Status().Code)
		require.Len(t, span.Events(), 1)
	})

	t.Run("predicates compose with OR", func(t *testing.T) {
		t.Parallel()
		span := runTrace(t, errUnexpected,
			SkipErrorRecordingIf(isExpected),
			SkipErrorRecordingIf(func(err error) bool { return errors.Is(err, errUnexpected) }),
		)
		require.Equal(t, codes.Unset, span.Status().Code)
		require.Empty(t, span.Events())
	})

	t.Run("no option records every error", func(t *testing.T) {
		t.Parallel()
		span := runTrace(t, errExpected)
		require.Equal(t, codes.Error, span.Status().Code)
		require.Len(t, span.Events(), 1)
	})
}

func TestTraceWithSpanStartOptions(t *testing.T) {
	t.Parallel()

	span := runTrace(t, errUnexpected,
		WithSpanStartOptions(trace.WithAttributes(attribute.String("a", "1"))),
		WithSpanStartOptions(trace.WithAttributes(attribute.String("b", "2"))),
	)
	require.ElementsMatch(t, []attribute.KeyValue{
		attribute.String("a", "1"),
		attribute.String("b", "2"),
	}, span.Attributes())
}
