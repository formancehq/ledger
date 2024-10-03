// Code generated by MockGen. DO NOT EDIT.
//
// Generated by this command:
//
//	mockgen -write_source_comment=false -write_package_comment=false -source machine_factory.go -destination machine_factory_generated_test.go -package ledger . MachineFactory
package ledger

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockMachineFactory is a mock of MachineFactory interface.
type MockMachineFactory struct {
	ctrl     *gomock.Controller
	recorder *MockMachineFactoryMockRecorder
}

// MockMachineFactoryMockRecorder is the mock recorder for MockMachineFactory.
type MockMachineFactoryMockRecorder struct {
	mock *MockMachineFactory
}

// NewMockMachineFactory creates a new mock instance.
func NewMockMachineFactory(ctrl *gomock.Controller) *MockMachineFactory {
	mock := &MockMachineFactory{ctrl: ctrl}
	mock.recorder = &MockMachineFactoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMachineFactory) EXPECT() *MockMachineFactoryMockRecorder {
	return m.recorder
}

// Make mocks base method.
func (m *MockMachineFactory) Make(script string) (Machine, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Make", script)
	ret0, _ := ret[0].(Machine)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Make indicates an expected call of Make.
func (mr *MockMachineFactoryMockRecorder) Make(script any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Make", reflect.TypeOf((*MockMachineFactory)(nil).Make), script)
}