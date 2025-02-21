// Code generated by MockGen. DO NOT EDIT.
//
// Generated by this command:
//
//	mockgen -write_source_comment=false -write_package_comment=false -source controller.go -destination controller_generated_test.go -package ledger . Controller
//

package ledger

import (
	context "context"
	sql "database/sql"
	reflect "reflect"

	bunpaginate "github.com/formancehq/go-libs/v2/bun/bunpaginate"
	migrations "github.com/formancehq/go-libs/v2/migrations"
	ledger "github.com/formancehq/ledger/internal"
	gomock "go.uber.org/mock/gomock"
)

// MockController is a mock of Controller interface.
type MockController struct {
	ctrl     *gomock.Controller
	recorder *MockControllerMockRecorder
	isgomock struct{}
}

// MockControllerMockRecorder is the mock recorder for MockController.
type MockControllerMockRecorder struct {
	mock *MockController
}

// NewMockController creates a new mock instance.
func NewMockController(ctrl *gomock.Controller) *MockController {
	mock := &MockController{ctrl: ctrl}
	mock.recorder = &MockControllerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockController) EXPECT() *MockControllerMockRecorder {
	return m.recorder
}

// BeginTX mocks base method.
func (m *MockController) BeginTX(ctx context.Context, options *sql.TxOptions) (Controller, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BeginTX", ctx, options)
	ret0, _ := ret[0].(Controller)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BeginTX indicates an expected call of BeginTX.
func (mr *MockControllerMockRecorder) BeginTX(ctx, options any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BeginTX", reflect.TypeOf((*MockController)(nil).BeginTX), ctx, options)
}

// Commit mocks base method.
func (m *MockController) Commit(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Commit", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Commit indicates an expected call of Commit.
func (mr *MockControllerMockRecorder) Commit(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Commit", reflect.TypeOf((*MockController)(nil).Commit), ctx)
}

// CountAccounts mocks base method.
func (m *MockController) CountAccounts(ctx context.Context, query ResourceQuery[any]) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CountAccounts", ctx, query)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CountAccounts indicates an expected call of CountAccounts.
func (mr *MockControllerMockRecorder) CountAccounts(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CountAccounts", reflect.TypeOf((*MockController)(nil).CountAccounts), ctx, query)
}

// CountTransactions mocks base method.
func (m *MockController) CountTransactions(ctx context.Context, query ResourceQuery[any]) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CountTransactions", ctx, query)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CountTransactions indicates an expected call of CountTransactions.
func (mr *MockControllerMockRecorder) CountTransactions(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CountTransactions", reflect.TypeOf((*MockController)(nil).CountTransactions), ctx, query)
}

// CreateTransaction mocks base method.
func (m *MockController) CreateTransaction(ctx context.Context, parameters Parameters[RunScript]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateTransaction", ctx, parameters)
	ret0, _ := ret[0].(*ledger.Log)
	ret1, _ := ret[1].(*ledger.CreatedTransaction)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// CreateTransaction indicates an expected call of CreateTransaction.
func (mr *MockControllerMockRecorder) CreateTransaction(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateTransaction", reflect.TypeOf((*MockController)(nil).CreateTransaction), ctx, parameters)
}

// DeleteAccountMetadata mocks base method.
func (m *MockController) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteAccountMetadata", ctx, parameters)
	ret0, _ := ret[0].(*ledger.Log)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteAccountMetadata indicates an expected call of DeleteAccountMetadata.
func (mr *MockControllerMockRecorder) DeleteAccountMetadata(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteAccountMetadata", reflect.TypeOf((*MockController)(nil).DeleteAccountMetadata), ctx, parameters)
}

// DeleteTransactionMetadata mocks base method.
func (m *MockController) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteTransactionMetadata", ctx, parameters)
	ret0, _ := ret[0].(*ledger.Log)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteTransactionMetadata indicates an expected call of DeleteTransactionMetadata.
func (mr *MockControllerMockRecorder) DeleteTransactionMetadata(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteTransactionMetadata", reflect.TypeOf((*MockController)(nil).DeleteTransactionMetadata), ctx, parameters)
}

// Export mocks base method.
func (m *MockController) Export(ctx context.Context, w ExportWriter) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Export", ctx, w)
	ret0, _ := ret[0].(error)
	return ret0
}

// Export indicates an expected call of Export.
func (mr *MockControllerMockRecorder) Export(ctx, w any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Export", reflect.TypeOf((*MockController)(nil).Export), ctx, w)
}

// GetAccount mocks base method.
func (m *MockController) GetAccount(ctx context.Context, query ResourceQuery[any]) (*ledger.Account, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAccount", ctx, query)
	ret0, _ := ret[0].(*ledger.Account)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAccount indicates an expected call of GetAccount.
func (mr *MockControllerMockRecorder) GetAccount(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAccount", reflect.TypeOf((*MockController)(nil).GetAccount), ctx, query)
}

// GetAggregatedBalances mocks base method.
func (m *MockController) GetAggregatedBalances(ctx context.Context, q ResourceQuery[GetAggregatedVolumesOptions]) (ledger.BalancesByAssets, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAggregatedBalances", ctx, q)
	ret0, _ := ret[0].(ledger.BalancesByAssets)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAggregatedBalances indicates an expected call of GetAggregatedBalances.
func (mr *MockControllerMockRecorder) GetAggregatedBalances(ctx, q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAggregatedBalances", reflect.TypeOf((*MockController)(nil).GetAggregatedBalances), ctx, q)
}

// GetMigrationsInfo mocks base method.
func (m *MockController) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMigrationsInfo", ctx)
	ret0, _ := ret[0].([]migrations.Info)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMigrationsInfo indicates an expected call of GetMigrationsInfo.
func (mr *MockControllerMockRecorder) GetMigrationsInfo(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMigrationsInfo", reflect.TypeOf((*MockController)(nil).GetMigrationsInfo), ctx)
}

// GetStats mocks base method.
func (m *MockController) GetStats(ctx context.Context) (Stats, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStats", ctx)
	ret0, _ := ret[0].(Stats)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStats indicates an expected call of GetStats.
func (mr *MockControllerMockRecorder) GetStats(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStats", reflect.TypeOf((*MockController)(nil).GetStats), ctx)
}

// GetTransaction mocks base method.
func (m *MockController) GetTransaction(ctx context.Context, query ResourceQuery[any]) (*ledger.Transaction, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTransaction", ctx, query)
	ret0, _ := ret[0].(*ledger.Transaction)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTransaction indicates an expected call of GetTransaction.
func (mr *MockControllerMockRecorder) GetTransaction(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTransaction", reflect.TypeOf((*MockController)(nil).GetTransaction), ctx, query)
}

// GetVolumesWithBalances mocks base method.
func (m *MockController) GetVolumesWithBalances(ctx context.Context, q OffsetPaginatedQuery[GetVolumesOptions]) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVolumesWithBalances", ctx, q)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetVolumesWithBalances indicates an expected call of GetVolumesWithBalances.
func (mr *MockControllerMockRecorder) GetVolumesWithBalances(ctx, q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVolumesWithBalances", reflect.TypeOf((*MockController)(nil).GetVolumesWithBalances), ctx, q)
}

// Import mocks base method.
func (m *MockController) Import(ctx context.Context, stream chan ledger.Log) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Import", ctx, stream)
	ret0, _ := ret[0].(error)
	return ret0
}

// Import indicates an expected call of Import.
func (mr *MockControllerMockRecorder) Import(ctx, stream any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Import", reflect.TypeOf((*MockController)(nil).Import), ctx, stream)
}

// IsDatabaseUpToDate mocks base method.
func (m *MockController) IsDatabaseUpToDate(ctx context.Context) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsDatabaseUpToDate", ctx)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IsDatabaseUpToDate indicates an expected call of IsDatabaseUpToDate.
func (mr *MockControllerMockRecorder) IsDatabaseUpToDate(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsDatabaseUpToDate", reflect.TypeOf((*MockController)(nil).IsDatabaseUpToDate), ctx)
}

// ListAccounts mocks base method.
func (m *MockController) ListAccounts(ctx context.Context, query OffsetPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Account], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListAccounts", ctx, query)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.Account])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListAccounts indicates an expected call of ListAccounts.
func (mr *MockControllerMockRecorder) ListAccounts(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListAccounts", reflect.TypeOf((*MockController)(nil).ListAccounts), ctx, query)
}

// ListLogs mocks base method.
func (m *MockController) ListLogs(ctx context.Context, query ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListLogs", ctx, query)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.Log])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListLogs indicates an expected call of ListLogs.
func (mr *MockControllerMockRecorder) ListLogs(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListLogs", reflect.TypeOf((*MockController)(nil).ListLogs), ctx, query)
}

// ListTransactions mocks base method.
func (m *MockController) ListTransactions(ctx context.Context, query ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Transaction], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListTransactions", ctx, query)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.Transaction])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListTransactions indicates an expected call of ListTransactions.
func (mr *MockControllerMockRecorder) ListTransactions(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListTransactions", reflect.TypeOf((*MockController)(nil).ListTransactions), ctx, query)
}

// RevertTransaction mocks base method.
func (m *MockController) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RevertTransaction", ctx, parameters)
	ret0, _ := ret[0].(*ledger.Log)
	ret1, _ := ret[1].(*ledger.RevertedTransaction)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// RevertTransaction indicates an expected call of RevertTransaction.
func (mr *MockControllerMockRecorder) RevertTransaction(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RevertTransaction", reflect.TypeOf((*MockController)(nil).RevertTransaction), ctx, parameters)
}

// Rollback mocks base method.
func (m *MockController) Rollback(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rollback", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Rollback indicates an expected call of Rollback.
func (mr *MockControllerMockRecorder) Rollback(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rollback", reflect.TypeOf((*MockController)(nil).Rollback), ctx)
}

// SaveAccountMetadata mocks base method.
func (m *MockController) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SaveAccountMetadata", ctx, parameters)
	ret0, _ := ret[0].(*ledger.Log)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SaveAccountMetadata indicates an expected call of SaveAccountMetadata.
func (mr *MockControllerMockRecorder) SaveAccountMetadata(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SaveAccountMetadata", reflect.TypeOf((*MockController)(nil).SaveAccountMetadata), ctx, parameters)
}

// SaveTransactionMetadata mocks base method.
func (m *MockController) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SaveTransactionMetadata", ctx, parameters)
	ret0, _ := ret[0].(*ledger.Log)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SaveTransactionMetadata indicates an expected call of SaveTransactionMetadata.
func (mr *MockControllerMockRecorder) SaveTransactionMetadata(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SaveTransactionMetadata", reflect.TypeOf((*MockController)(nil).SaveTransactionMetadata), ctx, parameters)
}
