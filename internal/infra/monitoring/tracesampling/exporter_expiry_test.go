package tracesampling

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func samplingSpan(traceID, spanID byte, code codes.Code) sdktrace.ReadOnlySpan {
	return tracetest.SpanStub{
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: oteltrace.TraceID{traceID}, SpanID: oteltrace.SpanID{spanID},
		}),
		Status: sdktrace.Status{Code: code},
	}.Snapshot()
}

func TestErrorAwareSamplingExporter_OnlyFlushesDiscoveredErrors(t *testing.T) {
	t.Parallel()
	delegate := &mockExporter{}
	exporter := NewErrorAwareSamplingExporter(delegate, 0)
	child := samplingSpan(1, 1, codes.Ok)
	other := samplingSpan(2, 2, codes.Ok)
	require.NoError(t, exporter.ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{child, other}))
	sibling := samplingSpan(1, 3, codes.Ok)
	parent := samplingSpan(1, 4, codes.Error)
	secondError := samplingSpan(1, 5, codes.Error)
	require.NoError(t, exporter.ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{sibling, parent, secondError}))
	require.Equal(t, []sdktrace.ReadOnlySpan{child, sibling, parent, secondError}, delegate.spans)
	require.Len(t, exporter.pending, 1)
	require.Equal(t, []sdktrace.ReadOnlySpan{other}, exporter.pending[other.SpanContext().TraceID()].spans)
	require.Len(t, exporter.expirations, 2)
	require.Len(t, exporter.expirations[1].errorIDs, 1, "enqueue only the first error for a trace")
}

func TestErrorAwareSamplingExporter_ExpiryBoundariesAndPromotion(t *testing.T) {
	t.Parallel()
	delegate := &mockExporter{}
	exporter := NewErrorAwareSamplingExporter(delegate, 0)
	child := samplingSpan(1, 1, codes.Ok)
	other := samplingSpan(2, 2, codes.Ok)
	parent := samplingSpan(1, 3, codes.Error)
	require.NoError(t, exporter.ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{child, other}))
	require.NoError(t, exporter.ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{parent}))
	require.Equal(t, []sdktrace.ReadOnlySpan{child, parent}, delegate.spans)

	// Set exact first-seen epochs to exercise expiry without sleeps. Expiration
	// is owned by the FIFO; keep map timestamps consistent with these epochs.
	first := time.Now().Add(-10 * time.Second)
	second := first.Add(5 * time.Second)
	exporter.expirations[0].firstSeen = first
	exporter.expirations[1].firstSeen = second
	exporter.pending[other.SpanContext().TraceID()].firstSeen = first
	exporter.errorTraces[parent.SpanContext().TraceID()] = second
	queued := exporter.expirations
	exporter.mu.Lock()
	exporter.cleanupLocked(first.Add(exporter.pendingWindow))
	exporter.mu.Unlock()
	require.Contains(t, exporter.pending, [16]byte(other.SpanContext().TraceID()), "the exact boundary is still retained")
	require.Len(t, exporter.expirations, 2)

	exporter.mu.Lock()
	exporter.cleanupLocked(first.Add(exporter.pendingWindow + time.Nanosecond))
	exporter.mu.Unlock()
	require.Empty(t, exporter.pending)
	require.Contains(t, exporter.errorTraces, [16]byte(parent.SpanContext().TraceID()), "pending expiry must not expire the later error decision")
	require.Len(t, exporter.expirations, 1)
	require.Equal(t, expiryBatch{}, queued[0], "consumed ID slices must not remain reachable")

	late := samplingSpan(1, 4, codes.Ok)
	require.NoError(t, exporter.ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{late, parent}))
	require.Equal(t, []sdktrace.ReadOnlySpan{child, parent, late, parent}, delegate.spans)
	require.Len(t, exporter.expirations, 1, "repeated errors must not refresh or enqueue expiry")
	require.Equal(t, second, exporter.expirations[0].firstSeen)
	exporter.mu.Lock()
	exporter.cleanupLocked(second.Add(exporter.pendingWindow + time.Nanosecond))
	exporter.mu.Unlock()
	require.Empty(t, exporter.errorTraces)
	require.Nil(t, exporter.expirations)
	require.Equal(t, expiryBatch{}, queued[1])

	// A reused trace ID starts a fresh pending epoch after both old records expire.
	require.NoError(t, exporter.ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{child}))
	require.Len(t, exporter.pending, 1)
	require.Len(t, exporter.expirations, 1)
	require.NoError(t, exporter.Shutdown(context.Background()))
	require.Nil(t, exporter.pending)
	require.Nil(t, exporter.errorTraces)
	require.Nil(t, exporter.expirations)
}

func TestErrorAwareSamplingExporter_PendingDeadlineDoesNotRefresh(t *testing.T) {
	t.Parallel()
	exporter := NewErrorAwareSamplingExporter(&mockExporter{}, 0)
	span := samplingSpan(1, 1, codes.Ok)
	require.NoError(t, exporter.ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{span}))
	first := exporter.expirations[0].firstSeen
	require.NoError(t, exporter.ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{span}))
	require.Len(t, exporter.expirations, 1)
	require.Len(t, exporter.expirations[0].pendingIDs, 1)
	require.Equal(t, first, exporter.expirations[0].firstSeen)
	exporter.mu.Lock()
	exporter.cleanupLocked(first.Add(exporter.pendingWindow + time.Nanosecond))
	exporter.mu.Unlock()
	require.Empty(t, exporter.pending)
	require.Nil(t, exporter.expirations)
}
