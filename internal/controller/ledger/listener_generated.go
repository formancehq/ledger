// Code generated by MockGen. DO NOT EDIT.
// Source: listener.go
//
// Generated by this command:
//
//	mockgen -source listener.go -destination listener_generated.go -package ledger . Listener
//

// Package ledger is a generated GoMock package.
package ledger

import (
	context "context"
	reflect "reflect"

	metadata "github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
	gomock "go.uber.org/mock/gomock"
)

// MockListener is a mock of Listener interface.
type MockListener struct {
	ctrl     *gomock.Controller
	recorder *MockListenerMockRecorder
}

// MockListenerMockRecorder is the mock recorder for MockListener.
type MockListenerMockRecorder struct {
	mock *MockListener
}

// NewMockListener creates a new mock instance.
func NewMockListener(ctrl *gomock.Controller) *MockListener {
	mock := &MockListener{ctrl: ctrl}
	mock.recorder = &MockListenerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockListener) EXPECT() *MockListenerMockRecorder {
	return m.recorder
}

// CommittedTransactions mocks base method.
func (m *MockListener) CommittedTransactions(ctx context.Context, ledger string, res ledger.Transaction, accountMetadata ledger.AccountMetadata) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "CommittedTransactions", ctx, ledger, res, accountMetadata)
}

// CommittedTransactions indicates an expected call of CommittedTransactions.
func (mr *MockListenerMockRecorder) CommittedTransactions(ctx, ledger, res, accountMetadata any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CommittedTransactions", reflect.TypeOf((*MockListener)(nil).CommittedTransactions), ctx, ledger, res, accountMetadata)
}

// DeletedMetadata mocks base method.
func (m *MockListener) DeletedMetadata(ctx context.Context, ledger, targetType string, targetID any, key string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "DeletedMetadata", ctx, ledger, targetType, targetID, key)
}

// DeletedMetadata indicates an expected call of DeletedMetadata.
func (mr *MockListenerMockRecorder) DeletedMetadata(ctx, ledger, targetType, targetID, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeletedMetadata", reflect.TypeOf((*MockListener)(nil).DeletedMetadata), ctx, ledger, targetType, targetID, key)
}

// RevertedTransaction mocks base method.
func (m *MockListener) RevertedTransaction(ctx context.Context, ledger string, reverted, revert ledger.Transaction) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RevertedTransaction", ctx, ledger, reverted, revert)
}

// RevertedTransaction indicates an expected call of RevertedTransaction.
func (mr *MockListenerMockRecorder) RevertedTransaction(ctx, ledger, reverted, revert any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RevertedTransaction", reflect.TypeOf((*MockListener)(nil).RevertedTransaction), ctx, ledger, reverted, revert)
}

// SavedMetadata mocks base method.
func (m *MockListener) SavedMetadata(ctx context.Context, ledger, targetType, id string, metadata metadata.Metadata) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SavedMetadata", ctx, ledger, targetType, id, metadata)
}

// SavedMetadata indicates an expected call of SavedMetadata.
func (mr *MockListenerMockRecorder) SavedMetadata(ctx, ledger, targetType, id, metadata any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SavedMetadata", reflect.TypeOf((*MockListener)(nil).SavedMetadata), ctx, ledger, targetType, id, metadata)
}
