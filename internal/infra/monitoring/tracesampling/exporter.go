package tracesampling

import (
	"context"
	"hash/fnv"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// ErrorAwareSamplingExporter is a SpanExporter that ensures all error spans
// are exported while applying ratio-based sampling to successful spans.
//
// This exporter wraps another SpanExporter and filters spans on export:
// - Spans with status code ERROR are always exported
// - Spans without errors are sampled based on the configured ratio
type ErrorAwareSamplingExporter struct {
	delegate sdktrace.SpanExporter
	ratio    float64
}

// NewErrorAwareSamplingExporter creates a new ErrorAwareSamplingExporter.
// The ratio parameter should be between 0.0 (no sampling) and 1.0 (sample all).
// Error spans are always exported regardless of the ratio.
func NewErrorAwareSamplingExporter(delegate sdktrace.SpanExporter, ratio float64) *ErrorAwareSamplingExporter {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	return &ErrorAwareSamplingExporter{
		delegate: delegate,
		ratio:    ratio,
	}
}

// ExportSpans exports the given spans after filtering.
// It uses a two-pass approach: first it identifies trace IDs that contain at
// least one error span, then it keeps ALL spans from those traces (so child
// spans are not dropped by ratio sampling when the parent has an error).
func (e *ErrorAwareSamplingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}

	// Pass 1: collect trace IDs that have at least one error span.
	errorTraceIDs := make(map[[16]byte]struct{})
	for _, s := range spans {
		if isErrorSpan(s) {
			errorTraceIDs[s.SpanContext().TraceID()] = struct{}{}
		}
	}

	// Pass 2: keep all spans from error traces + ratio-sampled non-error traces.
	filtered := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, s := range spans {
		traceID := s.SpanContext().TraceID()
		if _, ok := errorTraceIDs[traceID]; ok {
			filtered = append(filtered, s)
		} else if e.hashSample(traceID[:]) {
			filtered = append(filtered, s)
		}
	}

	if len(filtered) == 0 {
		return nil
	}

	return e.delegate.ExportSpans(ctx, filtered)
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

// Shutdown shuts down the exporter.
func (e *ErrorAwareSamplingExporter) Shutdown(ctx context.Context) error {
	return e.delegate.Shutdown(ctx)
}

var _ sdktrace.SpanExporter = (*ErrorAwareSamplingExporter)(nil)
