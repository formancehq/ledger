// Code generated by MockGen. DO NOT EDIT.
//
// Generated by this command:
//
//	mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package ledger . TX
package ledger

import (
	context "context"
	sql "database/sql"
	reflect "reflect"

	bunpaginate "github.com/formancehq/go-libs/v2/bun/bunpaginate"
	metadata "github.com/formancehq/go-libs/v2/metadata"
	migrations "github.com/formancehq/go-libs/v2/migrations"
	ledger "github.com/formancehq/ledger/internal"
	bun "github.com/uptrace/bun"
	gomock "go.uber.org/mock/gomock"
)

// MockTX is a mock of TX interface.
type MockTX struct {
	ctrl     *gomock.Controller
	recorder *MockTXMockRecorder
}

// MockTXMockRecorder is the mock recorder for MockTX.
type MockTXMockRecorder struct {
	mock *MockTX
}

// NewMockTX creates a new mock instance.
func NewMockTX(ctrl *gomock.Controller) *MockTX {
	mock := &MockTX{ctrl: ctrl}
	mock.recorder = &MockTXMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTX) EXPECT() *MockTXMockRecorder {
	return m.recorder
}

// CommitTransaction mocks base method.
func (m *MockTX) CommitTransaction(ctx context.Context, transaction *ledger.Transaction) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CommitTransaction", ctx, transaction)
	ret0, _ := ret[0].(error)
	return ret0
}

// CommitTransaction indicates an expected call of CommitTransaction.
func (mr *MockTXMockRecorder) CommitTransaction(ctx, transaction any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CommitTransaction", reflect.TypeOf((*MockTX)(nil).CommitTransaction), ctx, transaction)
}

// DeleteAccountMetadata mocks base method.
func (m *MockTX) DeleteAccountMetadata(ctx context.Context, address, key string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteAccountMetadata", ctx, address, key)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteAccountMetadata indicates an expected call of DeleteAccountMetadata.
func (mr *MockTXMockRecorder) DeleteAccountMetadata(ctx, address, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteAccountMetadata", reflect.TypeOf((*MockTX)(nil).DeleteAccountMetadata), ctx, address, key)
}

// DeleteTransactionMetadata mocks base method.
func (m *MockTX) DeleteTransactionMetadata(ctx context.Context, transactionID int, key string) (*ledger.Transaction, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteTransactionMetadata", ctx, transactionID, key)
	ret0, _ := ret[0].(*ledger.Transaction)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// DeleteTransactionMetadata indicates an expected call of DeleteTransactionMetadata.
func (mr *MockTXMockRecorder) DeleteTransactionMetadata(ctx, transactionID, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteTransactionMetadata", reflect.TypeOf((*MockTX)(nil).DeleteTransactionMetadata), ctx, transactionID, key)
}

// GetAccount mocks base method.
func (m *MockTX) GetAccount(ctx context.Context, query GetAccountQuery) (*ledger.Account, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAccount", ctx, query)
	ret0, _ := ret[0].(*ledger.Account)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAccount indicates an expected call of GetAccount.
func (mr *MockTXMockRecorder) GetAccount(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAccount", reflect.TypeOf((*MockTX)(nil).GetAccount), ctx, query)
}

// GetBalances mocks base method.
func (m *MockTX) GetBalances(ctx context.Context, query BalanceQuery) (Balances, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBalances", ctx, query)
	ret0, _ := ret[0].(Balances)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBalances indicates an expected call of GetBalances.
func (mr *MockTXMockRecorder) GetBalances(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBalances", reflect.TypeOf((*MockTX)(nil).GetBalances), ctx, query)
}

// InsertLog mocks base method.
func (m *MockTX) InsertLog(ctx context.Context, log *ledger.Log) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InsertLog", ctx, log)
	ret0, _ := ret[0].(error)
	return ret0
}

// InsertLog indicates an expected call of InsertLog.
func (mr *MockTXMockRecorder) InsertLog(ctx, log any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InsertLog", reflect.TypeOf((*MockTX)(nil).InsertLog), ctx, log)
}

// ListLogs mocks base method.
func (m *MockTX) ListLogs(ctx context.Context, q GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListLogs", ctx, q)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.Log])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListLogs indicates an expected call of ListLogs.
func (mr *MockTXMockRecorder) ListLogs(ctx, q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListLogs", reflect.TypeOf((*MockTX)(nil).ListLogs), ctx, q)
}

// LockLedger mocks base method.
func (m *MockTX) LockLedger(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LockLedger", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// LockLedger indicates an expected call of LockLedger.
func (mr *MockTXMockRecorder) LockLedger(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LockLedger", reflect.TypeOf((*MockTX)(nil).LockLedger), ctx)
}

// RevertTransaction mocks base method.
func (m *MockTX) RevertTransaction(ctx context.Context, id int) (*ledger.Transaction, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RevertTransaction", ctx, id)
	ret0, _ := ret[0].(*ledger.Transaction)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// RevertTransaction indicates an expected call of RevertTransaction.
func (mr *MockTXMockRecorder) RevertTransaction(ctx, id any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RevertTransaction", reflect.TypeOf((*MockTX)(nil).RevertTransaction), ctx, id)
}

// UpdateAccountsMetadata mocks base method.
func (m_2 *MockTX) UpdateAccountsMetadata(ctx context.Context, m map[string]metadata.Metadata) error {
	m_2.ctrl.T.Helper()
	ret := m_2.ctrl.Call(m_2, "UpdateAccountsMetadata", ctx, m)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateAccountsMetadata indicates an expected call of UpdateAccountsMetadata.
func (mr *MockTXMockRecorder) UpdateAccountsMetadata(ctx, m any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateAccountsMetadata", reflect.TypeOf((*MockTX)(nil).UpdateAccountsMetadata), ctx, m)
}

// UpdateTransactionMetadata mocks base method.
func (m_2 *MockTX) UpdateTransactionMetadata(ctx context.Context, transactionID int, m metadata.Metadata) (*ledger.Transaction, bool, error) {
	m_2.ctrl.T.Helper()
	ret := m_2.ctrl.Call(m_2, "UpdateTransactionMetadata", ctx, transactionID, m)
	ret0, _ := ret[0].(*ledger.Transaction)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// UpdateTransactionMetadata indicates an expected call of UpdateTransactionMetadata.
func (mr *MockTXMockRecorder) UpdateTransactionMetadata(ctx, transactionID, m any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateTransactionMetadata", reflect.TypeOf((*MockTX)(nil).UpdateTransactionMetadata), ctx, transactionID, m)
}

// UpsertAccount mocks base method.
func (m *MockTX) UpsertAccount(ctx context.Context, account *ledger.Account) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpsertAccount", ctx, account)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpsertAccount indicates an expected call of UpsertAccount.
func (mr *MockTXMockRecorder) UpsertAccount(ctx, account any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpsertAccount", reflect.TypeOf((*MockTX)(nil).UpsertAccount), ctx, account)
}

// MockStore is a mock of Store interface.
type MockStore struct {
	ctrl     *gomock.Controller
	recorder *MockStoreMockRecorder
}

// MockStoreMockRecorder is the mock recorder for MockStore.
type MockStoreMockRecorder struct {
	mock *MockStore
}

// NewMockStore creates a new mock instance.
func NewMockStore(ctrl *gomock.Controller) *MockStore {
	mock := &MockStore{ctrl: ctrl}
	mock.recorder = &MockStoreMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStore) EXPECT() *MockStoreMockRecorder {
	return m.recorder
}

// CountAccounts mocks base method.
func (m *MockStore) CountAccounts(ctx context.Context, a ListAccountsQuery) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CountAccounts", ctx, a)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CountAccounts indicates an expected call of CountAccounts.
func (mr *MockStoreMockRecorder) CountAccounts(ctx, a any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CountAccounts", reflect.TypeOf((*MockStore)(nil).CountAccounts), ctx, a)
}

// CountTransactions mocks base method.
func (m *MockStore) CountTransactions(ctx context.Context, q ListTransactionsQuery) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CountTransactions", ctx, q)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CountTransactions indicates an expected call of CountTransactions.
func (mr *MockStoreMockRecorder) CountTransactions(ctx, q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CountTransactions", reflect.TypeOf((*MockStore)(nil).CountTransactions), ctx, q)
}

// GetAccount mocks base method.
func (m *MockStore) GetAccount(ctx context.Context, q GetAccountQuery) (*ledger.Account, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAccount", ctx, q)
	ret0, _ := ret[0].(*ledger.Account)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAccount indicates an expected call of GetAccount.
func (mr *MockStoreMockRecorder) GetAccount(ctx, q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAccount", reflect.TypeOf((*MockStore)(nil).GetAccount), ctx, q)
}

// GetAggregatedBalances mocks base method.
func (m *MockStore) GetAggregatedBalances(ctx context.Context, q GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAggregatedBalances", ctx, q)
	ret0, _ := ret[0].(ledger.BalancesByAssets)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAggregatedBalances indicates an expected call of GetAggregatedBalances.
func (mr *MockStoreMockRecorder) GetAggregatedBalances(ctx, q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAggregatedBalances", reflect.TypeOf((*MockStore)(nil).GetAggregatedBalances), ctx, q)
}

// GetDB mocks base method.
func (m *MockStore) GetDB() bun.IDB {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDB")
	ret0, _ := ret[0].(bun.IDB)
	return ret0
}

// GetDB indicates an expected call of GetDB.
func (mr *MockStoreMockRecorder) GetDB() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDB", reflect.TypeOf((*MockStore)(nil).GetDB))
}

// GetMigrationsInfo mocks base method.
func (m *MockStore) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMigrationsInfo", ctx)
	ret0, _ := ret[0].([]migrations.Info)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMigrationsInfo indicates an expected call of GetMigrationsInfo.
func (mr *MockStoreMockRecorder) GetMigrationsInfo(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMigrationsInfo", reflect.TypeOf((*MockStore)(nil).GetMigrationsInfo), ctx)
}

// GetTransaction mocks base method.
func (m *MockStore) GetTransaction(ctx context.Context, query GetTransactionQuery) (*ledger.Transaction, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTransaction", ctx, query)
	ret0, _ := ret[0].(*ledger.Transaction)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTransaction indicates an expected call of GetTransaction.
func (mr *MockStoreMockRecorder) GetTransaction(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTransaction", reflect.TypeOf((*MockStore)(nil).GetTransaction), ctx, query)
}

// GetVolumesWithBalances mocks base method.
func (m *MockStore) GetVolumesWithBalances(ctx context.Context, q GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVolumesWithBalances", ctx, q)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetVolumesWithBalances indicates an expected call of GetVolumesWithBalances.
func (mr *MockStoreMockRecorder) GetVolumesWithBalances(ctx, q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVolumesWithBalances", reflect.TypeOf((*MockStore)(nil).GetVolumesWithBalances), ctx, q)
}

// IsUpToDate mocks base method.
func (m *MockStore) IsUpToDate(ctx context.Context) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsUpToDate", ctx)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IsUpToDate indicates an expected call of IsUpToDate.
func (mr *MockStoreMockRecorder) IsUpToDate(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsUpToDate", reflect.TypeOf((*MockStore)(nil).IsUpToDate), ctx)
}

// ListAccounts mocks base method.
func (m *MockStore) ListAccounts(ctx context.Context, a ListAccountsQuery) (*bunpaginate.Cursor[ledger.Account], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListAccounts", ctx, a)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.Account])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListAccounts indicates an expected call of ListAccounts.
func (mr *MockStoreMockRecorder) ListAccounts(ctx, a any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListAccounts", reflect.TypeOf((*MockStore)(nil).ListAccounts), ctx, a)
}

// ListLogs mocks base method.
func (m *MockStore) ListLogs(ctx context.Context, q GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListLogs", ctx, q)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.Log])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListLogs indicates an expected call of ListLogs.
func (mr *MockStoreMockRecorder) ListLogs(ctx, q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListLogs", reflect.TypeOf((*MockStore)(nil).ListLogs), ctx, q)
}

// ListTransactions mocks base method.
func (m *MockStore) ListTransactions(ctx context.Context, q ListTransactionsQuery) (*bunpaginate.Cursor[ledger.Transaction], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListTransactions", ctx, q)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.Transaction])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListTransactions indicates an expected call of ListTransactions.
func (mr *MockStoreMockRecorder) ListTransactions(ctx, q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListTransactions", reflect.TypeOf((*MockStore)(nil).ListTransactions), ctx, q)
}

// ReadLogWithIdempotencyKey mocks base method.
func (m *MockStore) ReadLogWithIdempotencyKey(ctx context.Context, ik string) (*ledger.Log, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadLogWithIdempotencyKey", ctx, ik)
	ret0, _ := ret[0].(*ledger.Log)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadLogWithIdempotencyKey indicates an expected call of ReadLogWithIdempotencyKey.
func (mr *MockStoreMockRecorder) ReadLogWithIdempotencyKey(ctx, ik any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadLogWithIdempotencyKey", reflect.TypeOf((*MockStore)(nil).ReadLogWithIdempotencyKey), ctx, ik)
}

// WithTX mocks base method.
func (m *MockStore) WithTX(arg0 context.Context, arg1 *sql.TxOptions, arg2 func(TX) (bool, error)) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WithTX", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// WithTX indicates an expected call of WithTX.
func (mr *MockStoreMockRecorder) WithTX(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WithTX", reflect.TypeOf((*MockStore)(nil).WithTX), arg0, arg1, arg2)
}
