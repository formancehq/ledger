// Code generated by MockGen. DO NOT EDIT.
//
// Generated by this command:
//
//	mockgen -write_source_comment=false -write_package_comment=false -source ../ledger/factory.go -destination ledger_generated_test.go -package driver --mock_names Factory=LedgerStoreFactory . Factory
package driver

import (
	reflect "reflect"

	ledger "github.com/formancehq/ledger/internal"
	bucket "github.com/formancehq/ledger/internal/storage/bucket"
	ledger0 "github.com/formancehq/ledger/internal/storage/ledger"
	gomock "go.uber.org/mock/gomock"
)

// LedgerStoreFactory is a mock of Factory interface.
type LedgerStoreFactory struct {
	ctrl     *gomock.Controller
	recorder *LedgerStoreFactoryMockRecorder
}

// LedgerStoreFactoryMockRecorder is the mock recorder for LedgerStoreFactory.
type LedgerStoreFactoryMockRecorder struct {
	mock *LedgerStoreFactory
}

// NewLedgerStoreFactory creates a new mock instance.
func NewLedgerStoreFactory(ctrl *gomock.Controller) *LedgerStoreFactory {
	mock := &LedgerStoreFactory{ctrl: ctrl}
	mock.recorder = &LedgerStoreFactoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *LedgerStoreFactory) EXPECT() *LedgerStoreFactoryMockRecorder {
	return m.recorder
}

// Create mocks base method.
func (m *LedgerStoreFactory) Create(arg0 bucket.Bucket, arg1 ledger.Ledger) *ledger0.Store {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Create", arg0, arg1)
	ret0, _ := ret[0].(*ledger0.Store)
	return ret0
}

// Create indicates an expected call of Create.
func (mr *LedgerStoreFactoryMockRecorder) Create(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Create", reflect.TypeOf((*LedgerStoreFactory)(nil).Create), arg0, arg1)
}