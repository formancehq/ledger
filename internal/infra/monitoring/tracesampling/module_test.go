package tracesampling

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
)

func TestModuleDisabledExportsCompleteBatches(t *testing.T) {
	t.Parallel()

	delegate := &mockExporter{}
	var exporter sdktrace.SpanExporter
	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() sdktrace.SpanExporter { return delegate }),
		Module(Config{Enabled: false, SuccessRatio: 0}),
		fx.Populate(&exporter),
	)
	require.NoError(t, app.Err())
	require.Same(t, delegate, exporter, "disabled sampling must bypass the wrapper entirely")

	child := samplingSpan(1, 1, codes.Ok)
	parent := samplingSpan(1, 2, codes.Error)
	require.NoError(t, exporter.ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{child}))
	require.Equal(t, []sdktrace.ReadOnlySpan{child}, delegate.spans, "children reach the collector before the error is known")
	require.NoError(t, exporter.ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{parent}))
	require.Equal(t, []sdktrace.ReadOnlySpan{child, parent}, delegate.spans)
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

	err = exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{})
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
