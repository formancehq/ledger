package tracesampling

import (
	"context"
	"hash/fnv"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// ErrorAwareSamplingProcessor is a SpanProcessor that ensures all error spans
// are exported while applying ratio-based sampling to successful spans.
//
// This processor wraps another SpanProcessor and filters spans on export:
// - Spans with status code ERROR are always exported
// - Spans without errors are sampled based on the configured ratio
type ErrorAwareSamplingProcessor struct {
	delegate sdktrace.SpanProcessor
	ratio    float64
}

// NewErrorAwareSamplingProcessor creates a new ErrorAwareSamplingProcessor.
// The ratio parameter should be between 0.0 (no sampling) and 1.0 (sample all).
// Error spans are always exported regardless of the ratio.
func NewErrorAwareSamplingProcessor(delegate sdktrace.SpanProcessor, ratio float64) *ErrorAwareSamplingProcessor {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	return &ErrorAwareSamplingProcessor{
		delegate: delegate,
		ratio:    ratio,
	}
}

// OnStart is called when a span is started.
func (p *ErrorAwareSamplingProcessor) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	// Always call delegate's OnStart - sampling decision is made on end
	p.delegate.OnStart(parent, s)
}

// OnEnd is called when a span is ended.
// This is where we apply the sampling logic.
func (p *ErrorAwareSamplingProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	if !p.shouldExport(s) {
		return
	}
	p.delegate.OnEnd(s)
}

// shouldExport determines whether a span should be exported.
// Error spans are always exported, successful spans are sampled based on ratio.
func (p *ErrorAwareSamplingProcessor) shouldExport(s sdktrace.ReadOnlySpan) bool {
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
	if p.ratio >= 1.0 {
		return true
	}
	if p.ratio <= 0 {
		return false
	}

	// Use trace ID for deterministic sampling
	traceID := s.SpanContext().TraceID()
	return p.hashSample(traceID[:])
}

// hashSample uses FNV-1a hash for deterministic sampling based on trace ID.
// This ensures that all spans within the same trace are either all sampled or all dropped.
func (p *ErrorAwareSamplingProcessor) hashSample(data []byte) bool {
	h := fnv.New64a()
	h.Write(data)
	hash := h.Sum64()

	// Convert ratio to threshold
	threshold := uint64(p.ratio * float64(^uint64(0)))
	return hash < threshold
}

// Shutdown shuts down the processor.
func (p *ErrorAwareSamplingProcessor) Shutdown(ctx context.Context) error {
	return p.delegate.Shutdown(ctx)
}

// ForceFlush forces the processor to flush any pending spans.
func (p *ErrorAwareSamplingProcessor) ForceFlush(ctx context.Context) error {
	return p.delegate.ForceFlush(ctx)
}

var _ sdktrace.SpanProcessor = (*ErrorAwareSamplingProcessor)(nil)
