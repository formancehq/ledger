package worker

import (
	"context"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger/internal/controller/system"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/mock/gomock"
)

type MockDriverWrapper struct {
	ctrl     *gomock.Controller
	recorder *MockDriverWrapperRecorder
}

type MockDriverWrapperRecorder struct {
	mock *MockDriverWrapper
}

func NewMockDriverWrapper(ctrl *gomock.Controller) *MockDriverWrapper {
	mock := &MockDriverWrapper{ctrl: ctrl}
	mock.recorder = &MockDriverWrapperRecorder{mock}
	return mock
}

func (m *MockDriverWrapper) EXPECT() *MockDriverWrapperRecorder {
	return m.recorder
}

func (m *MockDriverWrapper) GetBucketsMarkedForDeletion(ctx context.Context, days int) ([]string, error) {
	ret := m.ctrl.Call(m, "GetBucketsMarkedForDeletion", ctx, days)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockDriverWrapperRecorder) GetBucketsMarkedForDeletion(ctx, days interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBucketsMarkedForDeletion", nil, ctx, days)
}

func (m *MockDriverWrapper) PhysicallyDeleteBucket(ctx context.Context, bucketName string) error {
	ret := m.ctrl.Call(m, "PhysicallyDeleteBucket", ctx, bucketName)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockDriverWrapperRecorder) PhysicallyDeleteBucket(ctx, bucketName interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PhysicallyDeleteBucket", nil, ctx, bucketName)
}

func (m *MockDriverWrapper) ListBucketsWithStatus(ctx context.Context) ([]system.BucketWithStatus, error) {
	ret := m.ctrl.Call(m, "ListBucketsWithStatus", ctx)
	ret0, _ := ret[0].([]system.BucketWithStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockDriverWrapperRecorder) ListBucketsWithStatus(ctx interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListBucketsWithStatus", nil, ctx)
}

func (m *MockDriverWrapper) MarkBucketAsDeleted(ctx context.Context, bucketName string) error {
	ret := m.ctrl.Call(m, "MarkBucketAsDeleted", ctx, bucketName)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockDriverWrapperRecorder) MarkBucketAsDeleted(ctx, bucketName interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MarkBucketAsDeleted", nil, ctx, bucketName)
}

func (m *MockDriverWrapper) RestoreBucket(ctx context.Context, bucketName string) error {
	ret := m.ctrl.Call(m, "RestoreBucket", ctx, bucketName)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockDriverWrapperRecorder) RestoreBucket(ctx, bucketName interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RestoreBucket", nil, ctx, bucketName)
}

type MockTracer struct {
	ctrl         *gomock.Controller
	recorder     *MockTracerRecorder
	trace.Tracer // Embed the interface to satisfy it
}

func (m *MockTracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	varargs := []interface{}{ctx, spanName}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Start", varargs...)
	ret0, _ := ret[0].(context.Context)
	ret1, _ := ret[1].(trace.Span)
	return ret0, ret1
}

type MockTracerRecorder struct {
	mock *MockTracer
}

func NewMockTracer(ctrl *gomock.Controller) *MockTracer {
	mock := &MockTracer{ctrl: ctrl}
	mock.recorder = &MockTracerRecorder{mock}
	return mock
}

func (m *MockTracer) EXPECT() *MockTracerRecorder {
	return m.recorder
}

func (mr *MockTracerRecorder) Start(ctx, spanName interface{}, opts ...interface{}) *gomock.Call {
	varargs := []interface{}{ctx, spanName}
	varargs = append(varargs, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", nil, varargs...)
}

type MockSpan struct {
	ctrl     *gomock.Controller
	recorder *MockSpanRecorder
}

type MockSpanRecorder struct {
	mock *MockSpan
}

func NewMockSpan(ctrl *gomock.Controller) *MockSpan {
	mock := &MockSpan{ctrl: ctrl}
	mock.recorder = &MockSpanRecorder{mock}
	return mock
}

func (m *MockSpan) EXPECT() *MockSpanRecorder {
	return m.recorder
}

func (m *MockSpan) End(options ...trace.SpanEndOption) {
	m.ctrl.Call(m, "End")
}

func (mr *MockSpanRecorder) End() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "End", nil)
}

func (m *MockSpan) AddEvent(name string, options ...trace.EventOption) {
}

func (m *MockSpan) IsRecording() bool {
	return true
}

func (m *MockSpan) RecordError(err error, options ...trace.EventOption) {
}

func (m *MockSpan) SpanContext() trace.SpanContext {
	return trace.SpanContext{}
}

func (m *MockSpan) SetStatus(code codes.Code, description string) {
	m.ctrl.Call(m, "SetStatus", code, description)
}

func (mr *MockSpanRecorder) SetStatus(code, description interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetStatus", nil, code, description)
}

func (m *MockSpan) SetName(name string) {
	m.ctrl.Call(m, "SetName", name)
}

func (mr *MockSpanRecorder) SetName(name interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetName", nil, name)
}

func (m *MockSpan) SetAttributes(attrs ...attribute.KeyValue) {
	var varargs []interface{}
	for _, a := range attrs {
		varargs = append(varargs, a)
	}
	m.ctrl.Call(m, "SetAttributes", varargs...)
}

func (mr *MockSpanRecorder) SetAttributes(attrs ...interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetAttributes", nil, attrs...)
}

func (m *MockSpan) TracerProvider() trace.TracerProvider {
	return nil
}

func NoOpLogger() logging.Logger {
	return logging.NewDefaultLogger(nil, false, false, false)
}
