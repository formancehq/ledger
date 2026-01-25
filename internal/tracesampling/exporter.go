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
func (e *ErrorAwareSamplingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}

	filtered := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, span := range spans {
		if e.shouldExport(span) {
			filtered = append(filtered, span)
		}
	}

	if len(filtered) == 0 {
		return nil
	}

	return e.delegate.ExportSpans(ctx, filtered)
}

// shouldExport determines whether a span should be exported.
// Error spans are always exported, successful spans are sampled based on ratio.
func (e *ErrorAwareSamplingExporter) shouldExport(s sdktrace.ReadOnlySpan) bool {
	// Always export error spans
	if s.Status().Code == codes.Error {
		return true
	}

	// Check for any error-related attributes
	for _, attr := range s.Attributes() {
		if attr.Key == "error" && attr.Value.AsBool() {
			return true
		}
		if attr.Key == "exception.type" || attr.Key == "exception.message" {
			return true
		}
	}

	// Apply ratio-based sampling for non-error spans
	if e.ratio >= 1.0 {
		return true
	}
	if e.ratio <= 0 {
		return false
	}

	// Use trace ID for deterministic sampling
	traceID := s.SpanContext().TraceID()
	return e.hashSample(traceID[:])
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
