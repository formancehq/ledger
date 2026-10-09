// Package apitrace keeps API error logs and request spans correlated.
package apitrace

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/v5/pkg/observe"
)

// Fields returns trace identifiers suitable for structured logs when the
// request span is recording. A non-recording span deliberately contributes no
// identifiers: exporting identifiers without an accompanying trace makes the
// log correlation misleading.
func Fields(ctx context.Context) map[string]any {
	fields := map[string]any{}
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return fields
	}

	spanContext := span.SpanContext()
	fields["trace_id"] = spanContext.TraceID().String()
	fields["span_id"] = spanContext.SpanID().String()

	return fields
}

// RecordPanic records a recovered panic on a recording request span as the
// OpenTelemetry exception event (exception.type, exception.message,
// exception.stacktrace), so trace backends render it as an exception. The
// panic value is diagnostic only: callers must never return it to clients.
func RecordPanic(ctx context.Context, recovered any, stack []byte) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	span.AddEvent(semconv.ExceptionEventName, trace.WithAttributes(
		semconv.ExceptionType(fmt.Sprintf("%T", recovered)),
		semconv.ExceptionMessage(fmt.Sprintf("%v", recovered)),
		semconv.ExceptionStacktrace(string(stack)),
	))
}

// Stamp records the correlation ID and raw error on a recording request span.
func Stamp(ctx context.Context, correlationID string, err error) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	span.SetAttributes(attribute.String("correlation_id", correlationID))
	observe.RecordError(ctx, err)
}
