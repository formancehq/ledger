package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// This test changes SDK environment configuration, so it cannot run in parallel.
func TestAuthFailureExportsChildrenBeforeParentError(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	tracer := provider.Tracer("auth-test")

	// A caller's unsampled parent must not suppress Ledger spans when configured
	// for collector-side sampling. Preserve its trace ID and parent relationship.
	incoming := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, Remote: true,
	})
	ctx := trace.ContextWithRemoteSpanContext(t.Context(), incoming)
	parentCtx, parent := tracer.Start(ctx, "request")
	_, child := tracer.Start(parentCtx, "child")
	child.End()
	require.NoError(t, provider.ForceFlush(t.Context()))
	before := exporter.GetSpans()
	require.Len(t, before, 1, "successful child reaches the exporter before any error is known")
	require.Equal(t, child.SpanContext().SpanID(), before[0].SpanContext.SpanID())
	require.Equal(t, parent.SpanContext().SpanID(), before[0].Parent.SpanID())
	require.Equal(t, incoming.TraceID(), before[0].SpanContext.TraceID())
	require.True(t, before[0].SpanContext.IsSampled())

	failure := errors.New("token expired")
	logAuthFailure(parentCtx, "test-key", "invalid_token", failure)
	parent.End()
	require.NoError(t, provider.ForceFlush(t.Context()))
	spans := exporter.GetSpans()
	require.Len(t, spans, 2)
	require.Equal(t, before[0], spans[0], "the earlier successful span remains intact")
	errorSpan := spans[1]
	require.Equal(t, incoming.TraceID(), errorSpan.SpanContext.TraceID())
	require.Equal(t, incoming.SpanID(), errorSpan.Parent.SpanID())
	require.True(t, errorSpan.SpanContext.IsSampled())
	require.Equal(t, codes.Error, errorSpan.Status.Code)
	require.Equal(t, "auth failure: invalid_token", errorSpan.Status.Description)
	require.Contains(t, errorSpan.Attributes, attribute.String("auth.failure.reason", "invalid_token"))
	require.Contains(t, errorSpan.Attributes, attribute.String("auth.failure.error", failure.Error()))
	require.Contains(t, errorSpan.Attributes, attribute.String("auth.key_id", "test-key"))
	require.Len(t, errorSpan.Events, 1)
	require.Equal(t, "exception", errorSpan.Events[0].Name)
	require.Contains(t, errorSpan.Events[0].Attributes, attribute.String("exception.message", failure.Error()))
}
