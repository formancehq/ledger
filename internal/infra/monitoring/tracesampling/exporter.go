package tracesampling

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const defaultPendingWindow = 30 * time.Second

// ErrorAwareSamplingExporter is a SpanExporter that implements trace-aware
// tail-based sampling at the SDK level.
//
// It ensures that ALL spans from error traces are exported (including spans
// that arrived in earlier export batches), while applying ratio-based sampling
// to successful traces.
//
// This is necessary because child spans (e.g. ReadIndex, query) end in
// milliseconds and are batched for export long before the parent streaming
// span ends with an error. Without cross-batch buffering, those child spans
// would be dropped by ratio sampling.
//
// The approach:
//  1. Non-error, non-sampled spans are buffered for up to pendingWindow (30s)
//  2. When an error span arrives, its trace ID is recorded and all buffered
//     spans from that trace are flushed for export
//  3. Buffered spans whose trace never shows an error expire and are dropped
type ErrorAwareSamplingExporter struct {
	delegate      sdktrace.SpanExporter
	ratio         float64
	pendingWindow time.Duration

	mu          sync.Mutex
	errorTraces map[[16]byte]time.Time     // trace ID → when error was first seen
	pending     map[[16]byte]*pendingTrace // non-error, non-sampled spans awaiting decision
}

type pendingTrace struct {
	spans     []sdktrace.ReadOnlySpan
	firstSeen time.Time
}

// NewErrorAwareSamplingExporter creates a new ErrorAwareSamplingExporter.
// The ratio parameter should be between 0.0 (no sampling) and 1.0 (sample all).
// Error spans are always exported regardless of the ratio, along with all
// sibling spans from the same trace.
func NewErrorAwareSamplingExporter(delegate sdktrace.SpanExporter, ratio float64) *ErrorAwareSamplingExporter {
	if ratio < 0 {
		ratio = 0
	}

	if ratio > 1 {
		ratio = 1
	}

	return &ErrorAwareSamplingExporter{
		delegate:      delegate,
		ratio:         ratio,
		pendingWindow: defaultPendingWindow,
		errorTraces:   make(map[[16]byte]time.Time),
		pending:       make(map[[16]byte]*pendingTrace),
	}
}

// ExportSpans exports spans after applying trace-aware sampling.
//
// It uses a three-phase approach:
//  1. Discover error trace IDs in the current batch and record them
//  2. Flush any previously buffered spans whose trace is now known to have errors
//  3. For current batch: export error-trace spans and hash-sampled spans,
//     buffer the rest for future error discovery
func (e *ErrorAwareSamplingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}

	e.mu.Lock()

	now := time.Now()
	e.cleanupLocked(now)

	// Phase 1: discover error trace IDs in this batch.
	for _, s := range spans {
		if isErrorSpan(s) {
			traceID := s.SpanContext().TraceID()
			if _, ok := e.errorTraces[traceID]; !ok {
				e.errorTraces[traceID] = now
			}
		}
	}

	// Phase 2: flush previously buffered spans from now-known error traces.
	var toExport []sdktrace.ReadOnlySpan

	for id, pt := range e.pending {
		if _, ok := e.errorTraces[id]; ok {
			toExport = append(toExport, pt.spans...)

			delete(e.pending, id)
		}
	}

	// Phase 3: classify current batch spans.
	for _, s := range spans {
		traceID := s.SpanContext().TraceID()
		if _, ok := e.errorTraces[traceID]; ok {
			toExport = append(toExport, s)
		} else if e.hashSample(traceID[:]) {
			toExport = append(toExport, s)
		} else {
			// Buffer for future error discovery.
			pt, ok := e.pending[traceID]
			if !ok {
				pt = &pendingTrace{firstSeen: now}
				e.pending[traceID] = pt
			}

			pt.spans = append(pt.spans, s)
		}
	}

	e.mu.Unlock()

	if len(toExport) == 0 {
		return nil
	}

	return e.delegate.ExportSpans(ctx, toExport)
}

// cleanupLocked removes expired entries from errorTraces and pending.
// Must be called with e.mu held.
func (e *ErrorAwareSamplingExporter) cleanupLocked(now time.Time) {
	for id, t := range e.errorTraces {
		if now.Sub(t) > e.pendingWindow {
			delete(e.errorTraces, id)
		}
	}

	for id, pt := range e.pending {
		if now.Sub(pt.firstSeen) > e.pendingWindow {
			delete(e.pending, id)
		}
	}
}

// isErrorSpan returns true if the span indicates an error.
func isErrorSpan(s sdktrace.ReadOnlySpan) bool {
	if s.Status().Code == codes.Error {
		return true
	}

	for _, attr := range s.Attributes() {
		if attr.Key == "error" && attr.Value.AsBool() {
			return true
		}

		if attr.Key == "exception.type" || attr.Key == "exception.message" {
			return true
		}
	}

	return false
}

// hashSample uses FNV-1a hash for deterministic sampling based on trace ID.
// This ensures that all spans within the same trace are either all sampled or all dropped.
func (e *ErrorAwareSamplingExporter) hashSample(data []byte) bool {
	h := fnv.New64a()
	h.Write(data)
	hash := h.Sum64()

	// Convert ratio to threshold
	threshold := uint64(e.ratio * float64(^uint64(0)))

	return hash < threshold
}

// Shutdown flushes any remaining buffered spans from error traces, then
// shuts down the delegate exporter.
func (e *ErrorAwareSamplingExporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()

	var remaining []sdktrace.ReadOnlySpan

	for id, pt := range e.pending {
		if _, ok := e.errorTraces[id]; ok {
			remaining = append(remaining, pt.spans...)
		}
	}

	e.pending = nil
	e.errorTraces = nil
	e.mu.Unlock()

	if len(remaining) > 0 {
		_ = e.delegate.ExportSpans(ctx, remaining)
	}

	return e.delegate.Shutdown(ctx)
}

var _ sdktrace.SpanExporter = (*ErrorAwareSamplingExporter)(nil)
