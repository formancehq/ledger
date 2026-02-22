package tracesampling

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestWrapExporter_Enabled(t *testing.T) {
	t.Parallel()

	mock := &mockExporter{}
	cfg := Config{Enabled: true, SuccessRatio: 0.0}

	wrapped := WrapExporter(mock, cfg)

	// Should be a different type (ErrorAwareSamplingExporter)
	require.IsType(t, &ErrorAwareSamplingExporter{}, wrapped)

	// Error spans should pass through
	errorSpan := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Error},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:  oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		}),
	}.Snapshot()

	err := wrapped.ExportSpans(context.Background(), []trace.ReadOnlySpan{errorSpan})
	require.NoError(t, err)
	require.Len(t, mock.spans, 1)
}

func TestWrapExporter_Disabled(t *testing.T) {
	t.Parallel()

	mock := &mockExporter{}
	cfg := Config{Enabled: false, SuccessRatio: 0.5}

	wrapped := WrapExporter(mock, cfg)

	// Should be the same exporter (no wrapping)
	require.Same(t, mock, wrapped)
}

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	require.False(t, cfg.Enabled)
	require.Equal(t, 1.0, cfg.SuccessRatio)
}

func TestExporter_EmptySpans(t *testing.T) {
	t.Parallel()

	mock := &mockExporter{}
	exporter := NewErrorAwareSamplingExporter(mock, 0.5)

	err := exporter.ExportSpans(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, mock.spans)

	err = exporter.ExportSpans(context.Background(), []trace.ReadOnlySpan{})
	require.NoError(t, err)
	require.Empty(t, mock.spans)
}

func TestExporter_Shutdown_Delegates(t *testing.T) {
	t.Parallel()

	mock := &mockExporter{}
	exporter := NewErrorAwareSamplingExporter(mock, 0.5)

	err := exporter.Shutdown(context.Background())
	require.NoError(t, err)
}
