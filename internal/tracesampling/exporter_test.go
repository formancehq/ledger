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

type mockExporter struct {
	spans []trace.ReadOnlySpan
}

func (m *mockExporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	m.spans = append(m.spans, spans...)
	return nil
}

func (m *mockExporter) Shutdown(ctx context.Context) error {
	return nil
}

func TestErrorAwareSamplingExporter_AlwaysExportsErrors(t *testing.T) {
	mock := &mockExporter{}
	exporter := NewErrorAwareSamplingExporter(mock, 0.0) // 0% ratio for success

	// Create a span stub with error status
	errorSpan := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Error, Description: "test error"},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:  oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		}),
	}

	// Export the error span
	err := exporter.ExportSpans(context.Background(), []trace.ReadOnlySpan{errorSpan.Snapshot()})
	require.NoError(t, err)

	// Error span should be exported even with 0% ratio
	require.Len(t, mock.spans, 1, "error spans should always be exported")
}

func TestErrorAwareSamplingExporter_FiltersSuccessfulSpans(t *testing.T) {
	mock := &mockExporter{}
	exporter := NewErrorAwareSamplingExporter(mock, 0.0) // 0% ratio for success

	// Create a span stub with OK status
	okSpan := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Ok},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:  oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		}),
	}

	// Export the OK span
	err := exporter.ExportSpans(context.Background(), []trace.ReadOnlySpan{okSpan.Snapshot()})
	require.NoError(t, err)

	// Success span should NOT be exported with 0% ratio
	require.Len(t, mock.spans, 0, "successful spans should be filtered with 0% ratio")
}

func TestErrorAwareSamplingExporter_ExportsAllWithFullRatio(t *testing.T) {
	mock := &mockExporter{}
	exporter := NewErrorAwareSamplingExporter(mock, 1.0) // 100% ratio

	// Create span stubs
	spans := []trace.ReadOnlySpan{
		tracetest.SpanStub{
			Status: trace.Status{Code: codes.Ok},
			SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
				TraceID: oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
				SpanID:  oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
			}),
		}.Snapshot(),
		tracetest.SpanStub{
			Status: trace.Status{Code: codes.Unset},
			SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
				TraceID: oteltrace.TraceID{2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
				SpanID:  oteltrace.SpanID{2, 2, 3, 4, 5, 6, 7, 8},
			}),
		}.Snapshot(),
	}

	err := exporter.ExportSpans(context.Background(), spans)
	require.NoError(t, err)

	// All spans should be exported with 100% ratio
	require.Len(t, mock.spans, 2, "all spans should be exported with 100% ratio")
}

func TestErrorAwareSamplingExporter_DeterministicSampling(t *testing.T) {
	mock1 := &mockExporter{}
	mock2 := &mockExporter{}
	exporter1 := NewErrorAwareSamplingExporter(mock1, 0.5)
	exporter2 := NewErrorAwareSamplingExporter(mock2, 0.5)

	// Create a span with a specific trace ID
	span := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Ok},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:  oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		}),
	}.Snapshot()

	// Export the same span through both exporters
	_ = exporter1.ExportSpans(context.Background(), []trace.ReadOnlySpan{span})
	_ = exporter2.ExportSpans(context.Background(), []trace.ReadOnlySpan{span})

	// Both exporters should make the same decision for the same trace ID
	require.Equal(t, len(mock1.spans), len(mock2.spans),
		"same trace ID should have deterministic sampling result")
}

func TestErrorAwareSamplingExporter_RatioClamps(t *testing.T) {
	// Test that ratios outside [0, 1] are clamped
	exp1 := NewErrorAwareSamplingExporter(&mockExporter{}, -0.5)
	require.Equal(t, 0.0, exp1.ratio, "negative ratio should be clamped to 0")

	exp2 := NewErrorAwareSamplingExporter(&mockExporter{}, 1.5)
	require.Equal(t, 1.0, exp2.ratio, "ratio > 1 should be clamped to 1")
}
