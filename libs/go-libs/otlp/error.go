package otlp

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func RecordError(ctx context.Context, e error) {
	if e == nil {
		return
	}
	span := trace.SpanFromContext(ctx)
	span.SetStatus(codes.Error, e.Error())
	span.RecordError(e, trace.WithStackTrace(true))
}

func RecordAsError(ctx context.Context, e any) {
	if e == nil {
		return
	}
	span := trace.SpanFromContext(ctx)
	switch ee := e.(type) {
	case error:
		RecordError(ctx, ee)
	default:
		span.SetStatus(codes.Error, fmt.Sprint(e))
		span.RecordError(fmt.Errorf("%s", e), trace.WithStackTrace(true))
	}
}
