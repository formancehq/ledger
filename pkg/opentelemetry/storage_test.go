package opentelemetry

//
//import (
//	"context"
//	"github.com/opentracing/opentracing-go"
//	"github.com/opentracing/opentracing-go/mocktracer"
//	"github.com/stretchr/testify/assert"
//	"go.opentelemetry.io/otel/exporters/"
//	"go.opentelemetry.io/otel/trace"
//	"testing"
//)
//
//func TestOpentracingStorage_Initialize(t *testing.T) {
//
//	trace.NewNoopTracerProvider()
//
//	tracer := mocktracer.New()
//	opentracing.SetGlobalTracer(tracer)
//
//	store := NewOpentracingStorage(NoOpStore())
//	err := store.Initialize(context.Background())
//	assert.NoError(t, err)
//
//	fs := tracer.FinishedSpans()
//	assert.Len(t, fs, 1)
//	assert.Equal(t, opentracingOperationName("Initialize"), fs[0].OperationName)
//}
