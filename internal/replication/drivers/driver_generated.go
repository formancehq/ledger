// Code generated by MockGen. DO NOT EDIT.
// Source: driver.go
//
// Generated by this command:
//
//	mockgen -source driver.go -destination driver_generated.go -package drivers . Driver
//

// Package drivers is a generated GoMock package.
package drivers

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockDriver is a mock of Driver interface.
type MockDriver struct {
	ctrl     *gomock.Controller
	recorder *MockDriverMockRecorder
	isgomock struct{}
}

// MockDriverMockRecorder is the mock recorder for MockDriver.
type MockDriverMockRecorder struct {
	mock *MockDriver
}

// NewMockDriver creates a new mock instance.
func NewMockDriver(ctrl *gomock.Controller) *MockDriver {
	mock := &MockDriver{ctrl: ctrl}
	mock.recorder = &MockDriverMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDriver) EXPECT() *MockDriverMockRecorder {
	return m.recorder
}

// Accept mocks base method.
func (m *MockDriver) Accept(ctx context.Context, logs ...LogWithLedger) ([]error, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx}
	for _, a := range logs {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Accept", varargs...)
	ret0, _ := ret[0].([]error)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Accept indicates an expected call of Accept.
func (mr *MockDriverMockRecorder) Accept(ctx any, logs ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx}, logs...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Accept", reflect.TypeOf((*MockDriver)(nil).Accept), varargs...)
}

// Start mocks base method.
func (m *MockDriver) Start(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Start", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Start indicates an expected call of Start.
func (mr *MockDriverMockRecorder) Start(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockDriver)(nil).Start), ctx)
}

// Stop mocks base method.
func (m *MockDriver) Stop(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockDriverMockRecorder) Stop(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockDriver)(nil).Stop), ctx)
}
