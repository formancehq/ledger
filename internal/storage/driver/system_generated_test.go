// Code generated by MockGen. DO NOT EDIT.
//
// Generated by this command:
//
//	mockgen -write_source_comment=false -write_package_comment=false -source ../system/store.go -destination system_generated_test.go -package driver --mock_names Store=SystemStore . Store
package driver

import (
	context "context"
	reflect "reflect"

	bunpaginate "github.com/formancehq/go-libs/v2/bun/bunpaginate"
	metadata "github.com/formancehq/go-libs/v2/metadata"
	migrations "github.com/formancehq/go-libs/v2/migrations"
	internal "github.com/formancehq/ledger/internal"
	ledger "github.com/formancehq/ledger/internal/controller/ledger"
	gomock "go.uber.org/mock/gomock"
)

// SystemStore is a mock of Store interface.
type SystemStore struct {
	ctrl     *gomock.Controller
	recorder *SystemStoreMockRecorder
}

// SystemStoreMockRecorder is the mock recorder for SystemStore.
type SystemStoreMockRecorder struct {
	mock *SystemStore
}

// NewSystemStore creates a new mock instance.
func NewSystemStore(ctrl *gomock.Controller) *SystemStore {
	mock := &SystemStore{ctrl: ctrl}
	mock.recorder = &SystemStoreMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *SystemStore) EXPECT() *SystemStoreMockRecorder {
	return m.recorder
}

// CreateLedger mocks base method.
func (m *SystemStore) CreateLedger(ctx context.Context, l *internal.Ledger) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateLedger", ctx, l)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateLedger indicates an expected call of CreateLedger.
func (mr *SystemStoreMockRecorder) CreateLedger(ctx, l any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateLedger", reflect.TypeOf((*SystemStore)(nil).CreateLedger), ctx, l)
}

// DeleteLedgerMetadata mocks base method.
func (m *SystemStore) DeleteLedgerMetadata(ctx context.Context, name, key string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteLedgerMetadata", ctx, name, key)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteLedgerMetadata indicates an expected call of DeleteLedgerMetadata.
func (mr *SystemStoreMockRecorder) DeleteLedgerMetadata(ctx, name, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteLedgerMetadata", reflect.TypeOf((*SystemStore)(nil).DeleteLedgerMetadata), ctx, name, key)
}

// GetDistinctBuckets mocks base method.
func (m *SystemStore) GetDistinctBuckets(ctx context.Context) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDistinctBuckets", ctx)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDistinctBuckets indicates an expected call of GetDistinctBuckets.
func (mr *SystemStoreMockRecorder) GetDistinctBuckets(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDistinctBuckets", reflect.TypeOf((*SystemStore)(nil).GetDistinctBuckets), ctx)
}

// GetLedger mocks base method.
func (m *SystemStore) GetLedger(ctx context.Context, name string) (*internal.Ledger, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetLedger", ctx, name)
	ret0, _ := ret[0].(*internal.Ledger)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetLedger indicates an expected call of GetLedger.
func (mr *SystemStoreMockRecorder) GetLedger(ctx, name any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetLedger", reflect.TypeOf((*SystemStore)(nil).GetLedger), ctx, name)
}

// GetMigrator mocks base method.
func (m *SystemStore) GetMigrator(options ...migrations.Option) *migrations.Migrator {
	m.ctrl.T.Helper()
	varargs := []any{}
	for _, a := range options {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetMigrator", varargs...)
	ret0, _ := ret[0].(*migrations.Migrator)
	return ret0
}

// GetMigrator indicates an expected call of GetMigrator.
func (mr *SystemStoreMockRecorder) GetMigrator(options ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMigrator", reflect.TypeOf((*SystemStore)(nil).GetMigrator), options...)
}

// IsUpToDate mocks base method.
func (m *SystemStore) IsUpToDate(ctx context.Context) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsUpToDate", ctx)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IsUpToDate indicates an expected call of IsUpToDate.
func (mr *SystemStoreMockRecorder) IsUpToDate(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsUpToDate", reflect.TypeOf((*SystemStore)(nil).IsUpToDate), ctx)
}

// ListLedgers mocks base method.
func (m *SystemStore) ListLedgers(ctx context.Context, q ledger.ListLedgersQuery) (*bunpaginate.Cursor[internal.Ledger], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListLedgers", ctx, q)
	ret0, _ := ret[0].(*bunpaginate.Cursor[internal.Ledger])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListLedgers indicates an expected call of ListLedgers.
func (mr *SystemStoreMockRecorder) ListLedgers(ctx, q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListLedgers", reflect.TypeOf((*SystemStore)(nil).ListLedgers), ctx, q)
}

// Migrate mocks base method.
func (m *SystemStore) Migrate(ctx context.Context, options ...migrations.Option) error {
	m.ctrl.T.Helper()
	varargs := []any{ctx}
	for _, a := range options {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Migrate", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// Migrate indicates an expected call of Migrate.
func (mr *SystemStoreMockRecorder) Migrate(ctx any, options ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx}, options...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Migrate", reflect.TypeOf((*SystemStore)(nil).Migrate), varargs...)
}

// UpdateLedgerMetadata mocks base method.
func (m_2 *SystemStore) UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error {
	m_2.ctrl.T.Helper()
	ret := m_2.ctrl.Call(m_2, "UpdateLedgerMetadata", ctx, name, m)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateLedgerMetadata indicates an expected call of UpdateLedgerMetadata.
func (mr *SystemStoreMockRecorder) UpdateLedgerMetadata(ctx, name, m any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateLedgerMetadata", reflect.TypeOf((*SystemStore)(nil).UpdateLedgerMetadata), ctx, name, m)
}
