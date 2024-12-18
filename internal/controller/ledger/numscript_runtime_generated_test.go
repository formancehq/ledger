// Code generated by MockGen. DO NOT EDIT.
//
// Generated by this command:
//
//	mockgen -write_source_comment=false -write_package_comment=false -source numscript_runtime.go -destination numscript_runtime_generated_test.go -package ledger . NumscriptRuntime
//

package ledger

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockNumscriptRuntime is a mock of NumscriptRuntime interface.
type MockNumscriptRuntime struct {
	ctrl     *gomock.Controller
	recorder *MockNumscriptRuntimeMockRecorder
	isgomock struct{}
}

// MockNumscriptRuntimeMockRecorder is the mock recorder for MockNumscriptRuntime.
type MockNumscriptRuntimeMockRecorder struct {
	mock *MockNumscriptRuntime
}

// NewMockNumscriptRuntime creates a new mock instance.
func NewMockNumscriptRuntime(ctrl *gomock.Controller) *MockNumscriptRuntime {
	mock := &MockNumscriptRuntime{ctrl: ctrl}
	mock.recorder = &MockNumscriptRuntimeMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockNumscriptRuntime) EXPECT() *MockNumscriptRuntimeMockRecorder {
	return m.recorder
}

// Execute mocks base method.
func (m *MockNumscriptRuntime) Execute(arg0 context.Context, arg1 Store, arg2 map[string]string) (*NumscriptExecutionResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute", arg0, arg1, arg2)
	ret0, _ := ret[0].(*NumscriptExecutionResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Execute indicates an expected call of Execute.
func (mr *MockNumscriptRuntimeMockRecorder) Execute(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockNumscriptRuntime)(nil).Execute), arg0, arg1, arg2)
}
