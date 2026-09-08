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
// It exports error traces together with their unexpired buffered spans from
// earlier export batches, while applying ratio-based sampling to successful
// traces. Errors arriving after the pending window cannot recover expired spans.
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
	expirations []expiryBatch              // first-seen order, containing IDs only (never spans)
}

// Every map insertion belongs to one export batch. All entries have the same
// fixed lifetime, so expiration can consume this FIFO without scanning live maps.
type expiryBatch struct {
	firstSeen  time.Time
	pendingIDs [][16]byte
	errorIDs   [][16]byte
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

	var toExport []sdktrace.ReadOnlySpan
	expiry := expiryBatch{firstSeen: now}

	// Discover errors and flush only their buffered spans. Successful batches
	// must not scan the entire pending window looking for newly errored traces.
	for _, s := range spans {
		if isErrorSpan(s) {
			traceID := s.SpanContext().TraceID()
			if _, ok := e.errorTraces[traceID]; !ok {
				e.errorTraces[traceID] = now
				expiry.errorIDs = append(expiry.errorIDs, traceID)
			}

			if pt, ok := e.pending[traceID]; ok {
				toExport = append(toExport, pt.spans...)
				delete(e.pending, traceID)
			}
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
				expiry.pendingIDs = append(expiry.pendingIDs, traceID)
			}

			pt.spans = append(pt.spans, s)
		}
	}

	if len(expiry.pendingIDs) > 0 || len(expiry.errorIDs) > 0 {
		e.expirations = append(e.expirations, expiry)
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
	for len(e.expirations) > 0 && now.Sub(e.expirations[0].firstSeen) > e.pendingWindow {
		expiry := e.expirations[0]
		for _, id := range expiry.errorIDs {
			delete(e.errorTraces, id)
		}
		for _, id := range expiry.pendingIDs {
			// A promoted trace is already absent. It cannot become pending
			// again until its later error deadline has also expired.
			delete(e.pending, id)
		}
		// Release ID slices as well as map entries; consumed batches must not
		// remain reachable through the queue's backing array.
		e.expirations[0] = expiryBatch{}
		e.expirations = e.expirations[1:]
	}
	if len(e.expirations) == 0 {
		e.expirations = nil
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
	if e.ratio >= 1.0 {
		return true
	}

	if e.ratio <= 0.0 {
		return false
	}

	h := fnv.New64a()
	h.Write(data)
	hash := h.Sum64()

	// Convert ratio to threshold. The boundary cases (0.0 and 1.0)
	// are handled above to avoid float64→uint64 precision issues.
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
	e.expirations = nil
	e.mu.Unlock()

	if len(remaining) > 0 {
		_ = e.delegate.ExportSpans(ctx, remaining)
	}

	return e.delegate.Shutdown(ctx)
}

var _ sdktrace.SpanExporter = (*ErrorAwareSamplingExporter)(nil)
