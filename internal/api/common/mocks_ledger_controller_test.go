// Code generated by MockGen. DO NOT EDIT.
//
// Generated by this command:
//
//	mockgen -write_source_comment=false -write_package_comment=false -source ../../controller/ledger/controller.go -destination mocks_ledger_controller_test.go -package common --mock_names Controller=LedgerController . Controller
package common

import (
	context "context"
	sql "database/sql"
	reflect "reflect"

	bunpaginate "github.com/formancehq/go-libs/v2/bun/bunpaginate"
	migrations "github.com/formancehq/go-libs/v2/migrations"
	internal "github.com/formancehq/ledger/internal"
	ledger "github.com/formancehq/ledger/internal/controller/ledger"
	gomock "go.uber.org/mock/gomock"
)

// LedgerController is a mock of Controller interface.
type LedgerController struct {
	ctrl     *gomock.Controller
	recorder *LedgerControllerMockRecorder
}

// LedgerControllerMockRecorder is the mock recorder for LedgerController.
type LedgerControllerMockRecorder struct {
	mock *LedgerController
}

// NewLedgerController creates a new mock instance.
func NewLedgerController(ctrl *gomock.Controller) *LedgerController {
	mock := &LedgerController{ctrl: ctrl}
	mock.recorder = &LedgerControllerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *LedgerController) EXPECT() *LedgerControllerMockRecorder {
	return m.recorder
}

// BeginTX mocks base method.
func (m *LedgerController) BeginTX(ctx context.Context, options *sql.TxOptions) (ledger.Controller, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BeginTX", ctx, options)
	ret0, _ := ret[0].(ledger.Controller)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BeginTX indicates an expected call of BeginTX.
func (mr *LedgerControllerMockRecorder) BeginTX(ctx, options any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BeginTX", reflect.TypeOf((*LedgerController)(nil).BeginTX), ctx, options)
}

// Commit mocks base method.
func (m *LedgerController) Commit(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Commit", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Commit indicates an expected call of Commit.
func (mr *LedgerControllerMockRecorder) Commit(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Commit", reflect.TypeOf((*LedgerController)(nil).Commit), ctx)
}

// CountAccounts mocks base method.
func (m *LedgerController) CountAccounts(ctx context.Context, query ledger.ResourceQuery[any]) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CountAccounts", ctx, query)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CountAccounts indicates an expected call of CountAccounts.
func (mr *LedgerControllerMockRecorder) CountAccounts(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CountAccounts", reflect.TypeOf((*LedgerController)(nil).CountAccounts), ctx, query)
}

// CountTransactions mocks base method.
func (m *LedgerController) CountTransactions(ctx context.Context, query ledger.ResourceQuery[any]) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CountTransactions", ctx, query)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CountTransactions indicates an expected call of CountTransactions.
func (mr *LedgerControllerMockRecorder) CountTransactions(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CountTransactions", reflect.TypeOf((*LedgerController)(nil).CountTransactions), ctx, query)
}

// CreateTransaction mocks base method.
func (m *LedgerController) CreateTransaction(ctx context.Context, parameters ledger.Parameters[ledger.RunScript]) (*internal.Log, *internal.CreatedTransaction, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateTransaction", ctx, parameters)
	ret0, _ := ret[0].(*internal.Log)
	ret1, _ := ret[1].(*internal.CreatedTransaction)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// CreateTransaction indicates an expected call of CreateTransaction.
func (mr *LedgerControllerMockRecorder) CreateTransaction(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateTransaction", reflect.TypeOf((*LedgerController)(nil).CreateTransaction), ctx, parameters)
}

// DeleteAccountMetadata mocks base method.
func (m *LedgerController) DeleteAccountMetadata(ctx context.Context, parameters ledger.Parameters[ledger.DeleteAccountMetadata]) (*internal.Log, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteAccountMetadata", ctx, parameters)
	ret0, _ := ret[0].(*internal.Log)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteAccountMetadata indicates an expected call of DeleteAccountMetadata.
func (mr *LedgerControllerMockRecorder) DeleteAccountMetadata(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteAccountMetadata", reflect.TypeOf((*LedgerController)(nil).DeleteAccountMetadata), ctx, parameters)
}

// DeleteTransactionMetadata mocks base method.
func (m *LedgerController) DeleteTransactionMetadata(ctx context.Context, parameters ledger.Parameters[ledger.DeleteTransactionMetadata]) (*internal.Log, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteTransactionMetadata", ctx, parameters)
	ret0, _ := ret[0].(*internal.Log)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteTransactionMetadata indicates an expected call of DeleteTransactionMetadata.
func (mr *LedgerControllerMockRecorder) DeleteTransactionMetadata(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteTransactionMetadata", reflect.TypeOf((*LedgerController)(nil).DeleteTransactionMetadata), ctx, parameters)
}

// Export mocks base method.
func (m *LedgerController) Export(ctx context.Context, w ledger.ExportWriter) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Export", ctx, w)
	ret0, _ := ret[0].(error)
	return ret0
}

// Export indicates an expected call of Export.
func (mr *LedgerControllerMockRecorder) Export(ctx, w any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Export", reflect.TypeOf((*LedgerController)(nil).Export), ctx, w)
}

// GetAccount mocks base method.
func (m *LedgerController) GetAccount(ctx context.Context, query ledger.ResourceQuery[any]) (*internal.Account, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAccount", ctx, query)
	ret0, _ := ret[0].(*internal.Account)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAccount indicates an expected call of GetAccount.
func (mr *LedgerControllerMockRecorder) GetAccount(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAccount", reflect.TypeOf((*LedgerController)(nil).GetAccount), ctx, query)
}

// GetAggregatedBalances mocks base method.
func (m *LedgerController) GetAggregatedBalances(ctx context.Context, q ledger.ResourceQuery[ledger.GetAggregatedVolumesOptions]) (internal.BalancesByAssets, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAggregatedBalances", ctx, q)
	ret0, _ := ret[0].(internal.BalancesByAssets)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAggregatedBalances indicates an expected call of GetAggregatedBalances.
func (mr *LedgerControllerMockRecorder) GetAggregatedBalances(ctx, q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAggregatedBalances", reflect.TypeOf((*LedgerController)(nil).GetAggregatedBalances), ctx, q)
}

// GetMigrationsInfo mocks base method.
func (m *LedgerController) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMigrationsInfo", ctx)
	ret0, _ := ret[0].([]migrations.Info)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMigrationsInfo indicates an expected call of GetMigrationsInfo.
func (mr *LedgerControllerMockRecorder) GetMigrationsInfo(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMigrationsInfo", reflect.TypeOf((*LedgerController)(nil).GetMigrationsInfo), ctx)
}

// GetStats mocks base method.
func (m *LedgerController) GetStats(ctx context.Context) (ledger.Stats, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStats", ctx)
	ret0, _ := ret[0].(ledger.Stats)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStats indicates an expected call of GetStats.
func (mr *LedgerControllerMockRecorder) GetStats(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStats", reflect.TypeOf((*LedgerController)(nil).GetStats), ctx)
}

// GetTransaction mocks base method.
func (m *LedgerController) GetTransaction(ctx context.Context, query ledger.ResourceQuery[any]) (*internal.Transaction, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTransaction", ctx, query)
	ret0, _ := ret[0].(*internal.Transaction)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTransaction indicates an expected call of GetTransaction.
func (mr *LedgerControllerMockRecorder) GetTransaction(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTransaction", reflect.TypeOf((*LedgerController)(nil).GetTransaction), ctx, query)
}

// GetVolumesWithBalances mocks base method.
func (m *LedgerController) GetVolumesWithBalances(ctx context.Context, q ledger.OffsetPaginatedQuery[ledger.GetVolumesOptions]) (*bunpaginate.Cursor[internal.VolumesWithBalanceByAssetByAccount], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVolumesWithBalances", ctx, q)
	ret0, _ := ret[0].(*bunpaginate.Cursor[internal.VolumesWithBalanceByAssetByAccount])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetVolumesWithBalances indicates an expected call of GetVolumesWithBalances.
func (mr *LedgerControllerMockRecorder) GetVolumesWithBalances(ctx, q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVolumesWithBalances", reflect.TypeOf((*LedgerController)(nil).GetVolumesWithBalances), ctx, q)
}

// Import mocks base method.
func (m *LedgerController) Import(ctx context.Context, stream chan internal.Log) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Import", ctx, stream)
	ret0, _ := ret[0].(error)
	return ret0
}

// Import indicates an expected call of Import.
func (mr *LedgerControllerMockRecorder) Import(ctx, stream any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Import", reflect.TypeOf((*LedgerController)(nil).Import), ctx, stream)
}

// IsDatabaseUpToDate mocks base method.
func (m *LedgerController) IsDatabaseUpToDate(ctx context.Context) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsDatabaseUpToDate", ctx)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IsDatabaseUpToDate indicates an expected call of IsDatabaseUpToDate.
func (mr *LedgerControllerMockRecorder) IsDatabaseUpToDate(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsDatabaseUpToDate", reflect.TypeOf((*LedgerController)(nil).IsDatabaseUpToDate), ctx)
}

// ListAccounts mocks base method.
func (m *LedgerController) ListAccounts(ctx context.Context, query ledger.OffsetPaginatedQuery[any]) (*bunpaginate.Cursor[internal.Account], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListAccounts", ctx, query)
	ret0, _ := ret[0].(*bunpaginate.Cursor[internal.Account])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListAccounts indicates an expected call of ListAccounts.
func (mr *LedgerControllerMockRecorder) ListAccounts(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListAccounts", reflect.TypeOf((*LedgerController)(nil).ListAccounts), ctx, query)
}

// ListLogs mocks base method.
func (m *LedgerController) ListLogs(ctx context.Context, query ledger.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[internal.Log], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListLogs", ctx, query)
	ret0, _ := ret[0].(*bunpaginate.Cursor[internal.Log])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListLogs indicates an expected call of ListLogs.
func (mr *LedgerControllerMockRecorder) ListLogs(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListLogs", reflect.TypeOf((*LedgerController)(nil).ListLogs), ctx, query)
}

// ListTransactions mocks base method.
func (m *LedgerController) ListTransactions(ctx context.Context, query ledger.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[internal.Transaction], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListTransactions", ctx, query)
	ret0, _ := ret[0].(*bunpaginate.Cursor[internal.Transaction])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListTransactions indicates an expected call of ListTransactions.
func (mr *LedgerControllerMockRecorder) ListTransactions(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListTransactions", reflect.TypeOf((*LedgerController)(nil).ListTransactions), ctx, query)
}

// RevertTransaction mocks base method.
func (m *LedgerController) RevertTransaction(ctx context.Context, parameters ledger.Parameters[ledger.RevertTransaction]) (*internal.Log, *internal.RevertedTransaction, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RevertTransaction", ctx, parameters)
	ret0, _ := ret[0].(*internal.Log)
	ret1, _ := ret[1].(*internal.RevertedTransaction)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// RevertTransaction indicates an expected call of RevertTransaction.
func (mr *LedgerControllerMockRecorder) RevertTransaction(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RevertTransaction", reflect.TypeOf((*LedgerController)(nil).RevertTransaction), ctx, parameters)
}

// Rollback mocks base method.
func (m *LedgerController) Rollback(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rollback", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Rollback indicates an expected call of Rollback.
func (mr *LedgerControllerMockRecorder) Rollback(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rollback", reflect.TypeOf((*LedgerController)(nil).Rollback), ctx)
}

// SaveAccountMetadata mocks base method.
func (m *LedgerController) SaveAccountMetadata(ctx context.Context, parameters ledger.Parameters[ledger.SaveAccountMetadata]) (*internal.Log, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SaveAccountMetadata", ctx, parameters)
	ret0, _ := ret[0].(*internal.Log)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SaveAccountMetadata indicates an expected call of SaveAccountMetadata.
func (mr *LedgerControllerMockRecorder) SaveAccountMetadata(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SaveAccountMetadata", reflect.TypeOf((*LedgerController)(nil).SaveAccountMetadata), ctx, parameters)
}

// SaveTransactionMetadata mocks base method.
func (m *LedgerController) SaveTransactionMetadata(ctx context.Context, parameters ledger.Parameters[ledger.SaveTransactionMetadata]) (*internal.Log, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SaveTransactionMetadata", ctx, parameters)
	ret0, _ := ret[0].(*internal.Log)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SaveTransactionMetadata indicates an expected call of SaveTransactionMetadata.
func (mr *LedgerControllerMockRecorder) SaveTransactionMetadata(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SaveTransactionMetadata", reflect.TypeOf((*LedgerController)(nil).SaveTransactionMetadata), ctx, parameters)
}
