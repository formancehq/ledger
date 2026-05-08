package tracesampling

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

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
