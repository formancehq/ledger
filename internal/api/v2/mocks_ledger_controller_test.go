// Code generated by MockGen. DO NOT EDIT.
//
// Generated by this command:
//
//	mockgen -write_source_comment=false -write_package_comment=false -source ../../controller/ledger/controller.go -destination mocks_ledger_controller_test.go -package v2 --mock_names Controller=LedgerController . Controller
package v2

import (
	context "context"
	reflect "reflect"

	bunpaginate "github.com/formancehq/go-libs/v2/bun/bunpaginate"
	migrations "github.com/formancehq/go-libs/v2/migrations"
	ledger "github.com/formancehq/ledger/internal"
	ledger0 "github.com/formancehq/ledger/internal/controller/ledger"
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

// CountAccounts mocks base method.
func (m *LedgerController) CountAccounts(ctx context.Context, query ledger0.ListAccountsQuery) (int, error) {
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
func (m *LedgerController) CountTransactions(ctx context.Context, query ledger0.ListTransactionsQuery) (int, error) {
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
func (m *LedgerController) CreateTransaction(ctx context.Context, parameters ledger0.Parameters[ledger0.RunScript]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateTransaction", ctx, parameters)
	ret0, _ := ret[0].(*ledger.Log)
	ret1, _ := ret[1].(*ledger.CreatedTransaction)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// CreateTransaction indicates an expected call of CreateTransaction.
func (mr *LedgerControllerMockRecorder) CreateTransaction(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateTransaction", reflect.TypeOf((*LedgerController)(nil).CreateTransaction), ctx, parameters)
}

// DeleteAccountMetadata mocks base method.
func (m *LedgerController) DeleteAccountMetadata(ctx context.Context, parameters ledger0.Parameters[ledger0.DeleteAccountMetadata]) (*ledger.Log, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteAccountMetadata", ctx, parameters)
	ret0, _ := ret[0].(*ledger.Log)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteAccountMetadata indicates an expected call of DeleteAccountMetadata.
func (mr *LedgerControllerMockRecorder) DeleteAccountMetadata(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteAccountMetadata", reflect.TypeOf((*LedgerController)(nil).DeleteAccountMetadata), ctx, parameters)
}

// DeleteTransactionMetadata mocks base method.
func (m *LedgerController) DeleteTransactionMetadata(ctx context.Context, parameters ledger0.Parameters[ledger0.DeleteTransactionMetadata]) (*ledger.Log, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteTransactionMetadata", ctx, parameters)
	ret0, _ := ret[0].(*ledger.Log)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteTransactionMetadata indicates an expected call of DeleteTransactionMetadata.
func (mr *LedgerControllerMockRecorder) DeleteTransactionMetadata(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteTransactionMetadata", reflect.TypeOf((*LedgerController)(nil).DeleteTransactionMetadata), ctx, parameters)
}

// Export mocks base method.
func (m *LedgerController) Export(ctx context.Context, w ledger0.ExportWriter) error {
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
func (m *LedgerController) GetAccount(ctx context.Context, query ledger0.GetAccountQuery) (*ledger.Account, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAccount", ctx, query)
	ret0, _ := ret[0].(*ledger.Account)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAccount indicates an expected call of GetAccount.
func (mr *LedgerControllerMockRecorder) GetAccount(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAccount", reflect.TypeOf((*LedgerController)(nil).GetAccount), ctx, query)
}

// GetAggregatedBalances mocks base method.
func (m *LedgerController) GetAggregatedBalances(ctx context.Context, q ledger0.GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAggregatedBalances", ctx, q)
	ret0, _ := ret[0].(ledger.BalancesByAssets)
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
func (m *LedgerController) GetStats(ctx context.Context) (ledger0.Stats, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStats", ctx)
	ret0, _ := ret[0].(ledger0.Stats)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStats indicates an expected call of GetStats.
func (mr *LedgerControllerMockRecorder) GetStats(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStats", reflect.TypeOf((*LedgerController)(nil).GetStats), ctx)
}

// GetTransaction mocks base method.
func (m *LedgerController) GetTransaction(ctx context.Context, query ledger0.GetTransactionQuery) (*ledger.Transaction, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTransaction", ctx, query)
	ret0, _ := ret[0].(*ledger.Transaction)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTransaction indicates an expected call of GetTransaction.
func (mr *LedgerControllerMockRecorder) GetTransaction(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTransaction", reflect.TypeOf((*LedgerController)(nil).GetTransaction), ctx, query)
}

// GetVolumesWithBalances mocks base method.
func (m *LedgerController) GetVolumesWithBalances(ctx context.Context, q ledger0.GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVolumesWithBalances", ctx, q)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetVolumesWithBalances indicates an expected call of GetVolumesWithBalances.
func (mr *LedgerControllerMockRecorder) GetVolumesWithBalances(ctx, q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVolumesWithBalances", reflect.TypeOf((*LedgerController)(nil).GetVolumesWithBalances), ctx, q)
}

// Import mocks base method.
func (m *LedgerController) Import(ctx context.Context, stream chan ledger.Log) error {
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
func (m *LedgerController) ListAccounts(ctx context.Context, query ledger0.ListAccountsQuery) (*bunpaginate.Cursor[ledger.Account], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListAccounts", ctx, query)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.Account])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListAccounts indicates an expected call of ListAccounts.
func (mr *LedgerControllerMockRecorder) ListAccounts(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListAccounts", reflect.TypeOf((*LedgerController)(nil).ListAccounts), ctx, query)
}

// ListLogs mocks base method.
func (m *LedgerController) ListLogs(ctx context.Context, query ledger0.GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListLogs", ctx, query)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.Log])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListLogs indicates an expected call of ListLogs.
func (mr *LedgerControllerMockRecorder) ListLogs(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListLogs", reflect.TypeOf((*LedgerController)(nil).ListLogs), ctx, query)
}

// ListTransactions mocks base method.
func (m *LedgerController) ListTransactions(ctx context.Context, query ledger0.ListTransactionsQuery) (*bunpaginate.Cursor[ledger.Transaction], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListTransactions", ctx, query)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.Transaction])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListTransactions indicates an expected call of ListTransactions.
func (mr *LedgerControllerMockRecorder) ListTransactions(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListTransactions", reflect.TypeOf((*LedgerController)(nil).ListTransactions), ctx, query)
}

// RevertTransaction mocks base method.
func (m *LedgerController) RevertTransaction(ctx context.Context, parameters ledger0.Parameters[ledger0.RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RevertTransaction", ctx, parameters)
	ret0, _ := ret[0].(*ledger.Log)
	ret1, _ := ret[1].(*ledger.RevertedTransaction)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// RevertTransaction indicates an expected call of RevertTransaction.
func (mr *LedgerControllerMockRecorder) RevertTransaction(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RevertTransaction", reflect.TypeOf((*LedgerController)(nil).RevertTransaction), ctx, parameters)
}

// SaveAccountMetadata mocks base method.
func (m *LedgerController) SaveAccountMetadata(ctx context.Context, parameters ledger0.Parameters[ledger0.SaveAccountMetadata]) (*ledger.Log, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SaveAccountMetadata", ctx, parameters)
	ret0, _ := ret[0].(*ledger.Log)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SaveAccountMetadata indicates an expected call of SaveAccountMetadata.
func (mr *LedgerControllerMockRecorder) SaveAccountMetadata(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SaveAccountMetadata", reflect.TypeOf((*LedgerController)(nil).SaveAccountMetadata), ctx, parameters)
}

// SaveTransactionMetadata mocks base method.
func (m *LedgerController) SaveTransactionMetadata(ctx context.Context, parameters ledger0.Parameters[ledger0.SaveTransactionMetadata]) (*ledger.Log, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SaveTransactionMetadata", ctx, parameters)
	ret0, _ := ret[0].(*ledger.Log)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SaveTransactionMetadata indicates an expected call of SaveTransactionMetadata.
func (mr *LedgerControllerMockRecorder) SaveTransactionMetadata(ctx, parameters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SaveTransactionMetadata", reflect.TypeOf((*LedgerController)(nil).SaveTransactionMetadata), ctx, parameters)
}
