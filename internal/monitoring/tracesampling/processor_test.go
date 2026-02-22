package tracesampling

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type mockProcessor struct {
	onStartCalls  int
	onEndSpans    []trace.ReadOnlySpan
	shutdownErr   error
	forceFlushErr error
	shutdownCalls int
	flushCalls    int
}

func (m *mockProcessor) OnStart(_ context.Context, _ trace.ReadWriteSpan) {
	m.onStartCalls++
}

func (m *mockProcessor) OnEnd(s trace.ReadOnlySpan) {
	m.onEndSpans = append(m.onEndSpans, s)
}

func (m *mockProcessor) Shutdown(ctx context.Context) error {
	m.shutdownCalls++
	return m.shutdownErr
}

func (m *mockProcessor) ForceFlush(ctx context.Context) error {
	m.flushCalls++
	return m.forceFlushErr
}

func TestProcessor_OnStart_Delegates(t *testing.T) {
	t.Parallel()

	mock := &mockProcessor{}
	p := NewErrorAwareSamplingProcessor(mock, 0.0)

	p.OnStart(context.Background(), nil)
	require.Equal(t, 1, mock.onStartCalls)
}

func TestProcessor_OnEnd_AlwaysExportsErrors(t *testing.T) {
	t.Parallel()

	mock := &mockProcessor{}
	p := NewErrorAwareSamplingProcessor(mock, 0.0)

	errorSpan := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Error, Description: "err"},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:  oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		}),
	}.Snapshot()

	p.OnEnd(errorSpan)
	require.Len(t, mock.onEndSpans, 1)
}

func TestProcessor_OnEnd_FiltersSuccessful(t *testing.T) {
	t.Parallel()

	mock := &mockProcessor{}
	p := NewErrorAwareSamplingProcessor(mock, 0.0)

	okSpan := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Ok},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:  oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		}),
	}.Snapshot()

	p.OnEnd(okSpan)
	require.Empty(t, mock.onEndSpans)
}

func TestProcessor_OnEnd_ExportsAllWithFullRatio(t *testing.T) {
	t.Parallel()

	mock := &mockProcessor{}
	p := NewErrorAwareSamplingProcessor(mock, 1.0)

	okSpan := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Ok},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:  oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		}),
	}.Snapshot()

	p.OnEnd(okSpan)
	require.Len(t, mock.onEndSpans, 1)
}

func TestProcessor_OnEnd_ErrorAttributeBool(t *testing.T) {
	t.Parallel()

	mock := &mockProcessor{}
	p := NewErrorAwareSamplingProcessor(mock, 0.0)

	span := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Ok},
		Attributes: []attribute.KeyValue{
			attribute.Bool("error", true),
		},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:  oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		}),
	}.Snapshot()

	p.OnEnd(span)
	require.Len(t, mock.onEndSpans, 1)
}

func TestProcessor_OnEnd_ExceptionType(t *testing.T) {
	t.Parallel()

	mock := &mockProcessor{}
	p := NewErrorAwareSamplingProcessor(mock, 0.0)

	span := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Ok},
		Attributes: []attribute.KeyValue{
			attribute.String("exception.type", "RuntimeError"),
		},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: oteltrace.TraceID{3, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:  oteltrace.SpanID{3, 2, 3, 4, 5, 6, 7, 8},
		}),
	}.Snapshot()

	p.OnEnd(span)
	require.Len(t, mock.onEndSpans, 1)
}

func TestProcessor_OnEnd_ExceptionMessage(t *testing.T) {
	t.Parallel()

	mock := &mockProcessor{}
	p := NewErrorAwareSamplingProcessor(mock, 0.0)

	span := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Ok},
		Attributes: []attribute.KeyValue{
			attribute.String("exception.message", "something failed"),
		},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: oteltrace.TraceID{4, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:  oteltrace.SpanID{4, 2, 3, 4, 5, 6, 7, 8},
		}),
	}.Snapshot()

	p.OnEnd(span)
	require.Len(t, mock.onEndSpans, 1)
}

func TestProcessor_RatioClamps(t *testing.T) {
	t.Parallel()

	p1 := NewErrorAwareSamplingProcessor(&mockProcessor{}, -0.5)
	require.Equal(t, 0.0, p1.ratio)

	p2 := NewErrorAwareSamplingProcessor(&mockProcessor{}, 1.5)
	require.Equal(t, 1.0, p2.ratio)
}

func TestProcessor_Shutdown_Delegates(t *testing.T) {
	t.Parallel()

	mock := &mockProcessor{shutdownErr: errors.New("shutdown err")}
	p := NewErrorAwareSamplingProcessor(mock, 0.5)

	err := p.Shutdown(context.Background())
	require.ErrorContains(t, err, "shutdown err")
	require.Equal(t, 1, mock.shutdownCalls)
}

func TestProcessor_ForceFlush_Delegates(t *testing.T) {
	t.Parallel()

	mock := &mockProcessor{forceFlushErr: errors.New("flush err")}
	p := NewErrorAwareSamplingProcessor(mock, 0.5)

	err := p.ForceFlush(context.Background())
	require.ErrorContains(t, err, "flush err")
	require.Equal(t, 1, mock.flushCalls)
}

func TestProcessor_DeterministicSampling(t *testing.T) {
	t.Parallel()

	mock1 := &mockProcessor{}
	mock2 := &mockProcessor{}
	p1 := NewErrorAwareSamplingProcessor(mock1, 0.5)
	p2 := NewErrorAwareSamplingProcessor(mock2, 0.5)

	span := tracetest.SpanStub{
		Status: trace.Status{Code: codes.Ok},
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:  oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		}),
	}.Snapshot()

	p1.OnEnd(span)
	p2.OnEnd(span)

	require.Equal(t, len(mock1.onEndSpans), len(mock2.onEndSpans))
}
