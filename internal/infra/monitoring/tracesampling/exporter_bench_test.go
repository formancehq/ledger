package tracesampling

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// BenchmarkExportSpansPending isolates per-batch work from SDK recording and
// network export. Setup uses real exports to retain one span per rejected trace.
// The timed batch is hash-sampled so the pending population remains constant.
func BenchmarkExportSpansPending(b *testing.B) {
	for _, count := range []int{0, 10000, 100000, 216000} {
		b.Run(fmt.Sprintf("traces=%d", count), func(b *testing.B) {
			exporter := NewErrorAwareSamplingExporter(tracetest.NewNoopExporter(), 0.1)
			exporter.pendingWindow = time.Hour // Prevent expiry during benchmark calibration.
			var retained []sdktrace.ReadOnlySpan
			var sampled sdktrace.ReadOnlySpan
			for i := uint64(1); len(retained) < count || sampled == nil; i++ {
				var id oteltrace.TraceID
				binary.LittleEndian.PutUint64(id[:8], i)
				span := tracetest.SpanStub{SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
					TraceID: id, SpanID: oteltrace.SpanID{1},
				})}.Snapshot()
				if exporter.hashSample(id[:]) {
					sampled = span
				} else if len(retained) < count {
					retained = append(retained, span)
				}
			}
			if err := exporter.ExportSpans(b.Context(), retained); err != nil {
				b.Fatal(err)
			}
			batch := make([]sdktrace.ReadOnlySpan, 512)
			for i := range batch {
				batch[i] = sampled
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := exporter.ExportSpans(context.Background(), batch); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if len(exporter.pending) != count {
				b.Fatalf("pending population changed: %d", len(exporter.pending))
			}
		})
	}
}
