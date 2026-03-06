package tracesampling

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type mockExporter struct {
	spans []trace.ReadOnlySpan
}

func (m *mockExporter) ExportSpans(_ context.Context, spans []trace.ReadOnlySpan) error {
	m.spans = append(m.spans, spans...)

	return nil
}

func (m *mockExporter) Shutdown(_ context.Context) error {
	return nil
}

func TestErrorAwareSamplingExporter_AlwaysExportsErrors(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

	// Export the OK span — it should be buffered, not exported
	err := exporter.ExportSpans(context.Background(), []trace.ReadOnlySpan{okSpan.Snapshot()})
	require.NoError(t, err)

	require.Empty(t, mock.spans, "successful spans should be buffered (not exported) with 0% ratio")
}

func TestErrorAwareSamplingExporter_ExportsAllWithFullRatio(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	require.Len(t, mock2.spans, len(mock1.spans),
		"same trace ID should have deterministic sampling result")
}

func TestErrorAwareSamplingExporter_KeepsSiblingSpansOfErrors(t *testing.T) {
	t.Parallel()

	mock := &mockExporter{}
	exporter := NewErrorAwareSamplingExporter(mock, 0.0) // 0% ratio for success

	traceID := oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Create an error span and a sibling OK span with the same trace ID.
	errorSpan := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Error, Description: "test error"},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  oteltrace.SpanID{1, 0, 0, 0, 0, 0, 0, 0},
		}),
	}.Snapshot()

	okSpan := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Ok},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  oteltrace.SpanID{2, 0, 0, 0, 0, 0, 0, 0},
		}),
	}.Snapshot()

	err := exporter.ExportSpans(context.Background(), []trace.ReadOnlySpan{errorSpan, okSpan})
	require.NoError(t, err)

	// Both spans should be exported because they share a trace ID with an error span
	require.Len(t, mock.spans, 2, "sibling spans of error spans should be kept regardless of ratio")
}

func TestErrorAwareSamplingExporter_CrossBatchErrorDiscovery(t *testing.T) {
	t.Parallel()

	mock := &mockExporter{}
	exporter := NewErrorAwareSamplingExporter(mock, 0.0) // 0% ratio — nothing passes hashSample

	traceID := oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Batch 1: child spans arrive first (no error yet).
	childSpan1 := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Ok},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  oteltrace.SpanID{1, 0, 0, 0, 0, 0, 0, 0},
		}),
	}.Snapshot()
	childSpan2 := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Ok},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  oteltrace.SpanID{2, 0, 0, 0, 0, 0, 0, 0},
		}),
	}.Snapshot()

	err := exporter.ExportSpans(context.Background(), []trace.ReadOnlySpan{childSpan1, childSpan2})
	require.NoError(t, err)
	require.Empty(t, mock.spans, "child spans should be buffered when no error is known")

	// Batch 2: error parent span arrives later.
	errorSpan := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Error, Description: "deadline exceeded"},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  oteltrace.SpanID{3, 0, 0, 0, 0, 0, 0, 0},
		}),
	}.Snapshot()

	err = exporter.ExportSpans(context.Background(), []trace.ReadOnlySpan{errorSpan})
	require.NoError(t, err)

	// All 3 spans should be exported: 2 flushed from buffer + 1 error span
	require.Len(t, mock.spans, 3, "buffered child spans should be flushed when error trace is discovered")
}

func TestErrorAwareSamplingExporter_ErrorTraceIDCachePersists(t *testing.T) {
	t.Parallel()

	mock := &mockExporter{}
	exporter := NewErrorAwareSamplingExporter(mock, 0.0)

	traceID := oteltrace.TraceID{5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5}

	// Batch 1: error span arrives first.
	errorSpan := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Error},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  oteltrace.SpanID{1, 0, 0, 0, 0, 0, 0, 0},
		}),
	}.Snapshot()

	err := exporter.ExportSpans(context.Background(), []trace.ReadOnlySpan{errorSpan})
	require.NoError(t, err)
	require.Len(t, mock.spans, 1)

	// Batch 2: late child span from same trace (e.g. grpc.ListLedgers ending).
	lateChild := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Ok},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  oteltrace.SpanID{2, 0, 0, 0, 0, 0, 0, 0},
		}),
	}.Snapshot()

	err = exporter.ExportSpans(context.Background(), []trace.ReadOnlySpan{lateChild})
	require.NoError(t, err)

	// The late child should also be exported (error trace ID is cached)
	require.Len(t, mock.spans, 2, "cached error trace IDs should keep late-arriving spans")
}

func TestErrorAwareSamplingExporter_PendingSpansExpire(t *testing.T) {
	t.Parallel()

	mock := &mockExporter{}
	exporter := NewErrorAwareSamplingExporter(mock, 0.0)
	exporter.pendingWindow = 1 * time.Millisecond // very short for testing

	traceID := oteltrace.TraceID{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}

	// Buffer a non-error span
	okSpan := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Ok},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  oteltrace.SpanID{1, 0, 0, 0, 0, 0, 0, 0},
		}),
	}.Snapshot()

	err := exporter.ExportSpans(context.Background(), []trace.ReadOnlySpan{okSpan})
	require.NoError(t, err)
	require.Empty(t, mock.spans)

	// Wait for the pending window to expire
	time.Sleep(5 * time.Millisecond)

	// Export another batch — cleanup should drop the expired pending span
	otherSpan := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Error},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: oteltrace.TraceID{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8},
			SpanID:  oteltrace.SpanID{1, 0, 0, 0, 0, 0, 0, 0},
		}),
	}.Snapshot()

	err = exporter.ExportSpans(context.Background(), []trace.ReadOnlySpan{otherSpan})
	require.NoError(t, err)

	// Only the other error span should be exported, not the expired one
	require.Len(t, mock.spans, 1, "expired pending spans should be dropped")

	// Verify the pending map is empty
	exporter.mu.Lock()
	require.Empty(t, exporter.pending, "expired pending entries should be cleaned up")
	exporter.mu.Unlock()
}

func TestErrorAwareSamplingExporter_ShutdownFlushesErrorTraces(t *testing.T) {
	t.Parallel()

	mock := &mockExporter{}
	exporter := NewErrorAwareSamplingExporter(mock, 0.0)

	traceIDError := oteltrace.TraceID{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	traceIDOk := oteltrace.TraceID{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}

	// Buffer spans from two traces
	spans := []trace.ReadOnlySpan{
		tracetest.SpanStub{
			Status: trace.Status{Code: codes.Ok},
			SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
				TraceID: traceIDError,
				SpanID:  oteltrace.SpanID{1, 0, 0, 0, 0, 0, 0, 0},
			}),
		}.Snapshot(),
		tracetest.SpanStub{
			Status: trace.Status{Code: codes.Ok},
			SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
				TraceID: traceIDOk,
				SpanID:  oteltrace.SpanID{2, 0, 0, 0, 0, 0, 0, 0},
			}),
		}.Snapshot(),
	}

	err := exporter.ExportSpans(context.Background(), spans)
	require.NoError(t, err)
	require.Empty(t, mock.spans, "both should be buffered")

	// Mark one trace as error (simulating error discovered before shutdown)
	exporter.mu.Lock()
	exporter.errorTraces[traceIDError] = time.Now()
	exporter.mu.Unlock()

	// Shutdown should flush only the error-trace spans
	err = exporter.Shutdown(context.Background())
	require.NoError(t, err)

	require.Len(t, mock.spans, 1, "shutdown should flush pending spans from error traces only")
}

func TestErrorAwareSamplingExporter_RatioClamps(t *testing.T) {
	t.Parallel()

	// Test that ratios outside [0, 1] are clamped
	exp1 := NewErrorAwareSamplingExporter(&mockExporter{}, -0.5)
	require.Equal(t, 0.0, exp1.ratio, "negative ratio should be clamped to 0")

	exp2 := NewErrorAwareSamplingExporter(&mockExporter{}, 1.5)
	require.Equal(t, 1.0, exp2.ratio, "ratio > 1 should be clamped to 1")
}
