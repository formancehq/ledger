package http

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// mockBackend implements Backend for testing.
type mockBackend struct {
	healthy bool
	ready   bool

	applyFn                   func(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error)
	listLedgersFn             func(ctx context.Context) (dal.Cursor[*commonpb.LedgerInfo], error)
	getLedgerByNameFn         func(ctx context.Context, name string) (*commonpb.LedgerInfo, error)
	getTransactionFn          func(ctx context.Context, ledgerName string, txID uint64) (*commonpb.Transaction, error)
	listTransactionsFn        func(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, reverse bool) (dal.Cursor[*commonpb.Transaction], error)
	getAccountFn              func(ctx context.Context, ledgerName string, address string) (*commonpb.Account, error)
	listAccountsFn            func(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter, reverse bool) (dal.Cursor[*commonpb.Account], error)
	listLogsFn                func(ctx context.Context, afterSequence uint64, pageSize uint32, filter *commonpb.QueryFilter) (dal.Cursor[*commonpb.Log], error)
	getLogFn                  func(ctx context.Context, sequence uint64) (*commonpb.Log, error)
	listAuditEntriesFn        func(ctx context.Context, afterSequence *uint64, failuresOnly bool, pageSize uint32) (dal.Cursor[*auditpb.AuditEntry], error)
	getAuditEntryFn           func(ctx context.Context, sequence uint64) (*auditpb.AuditEntry, error)
	listPeriodsFn             func(ctx context.Context) (dal.Cursor[*commonpb.Period], error)
	listSigningKeysFn         func(ctx context.Context) (dal.Cursor[*commonpb.SigningKey], error)
	getMetadataSchemaStatusFn func(ctx context.Context, ledgerName string) (*servicepb.GetMetadataSchemaStatusResponse, error)
	analyzeAccountsFn         func(ctx context.Context, ledgerName string, variableThreshold uint32) (*servicepb.AnalyzeAccountsResponse, error)
	analyzeTransactionsFn     func(ctx context.Context, ledgerName string, variableThreshold uint32) (*servicepb.AnalyzeTransactionsResponse, error)
	getClusterStateFn         func(ctx context.Context) (*clusterpb.ClusterState, error)
	getLedgerStatsFn          func(ctx context.Context, ledgerName string) (*commonpb.LedgerStats, error)
}

func (m *mockBackend) IsHealthy() bool { return m.healthy }
func (m *mockBackend) IsReady() bool   { return m.ready }
func (m *mockBackend) NotReadyReasons() []string {
	if m.ready {
		return nil
	}

	return []string{"mock: not ready"}
}

func (m *mockBackend) GetClusterState(ctx context.Context) (*clusterpb.ClusterState, error) {
	if m.getClusterStateFn != nil {
		return m.getClusterStateFn(ctx)
	}

	return nil, nil
}

func (m *mockBackend) Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
	if m.applyFn != nil {
		return m.applyFn(ctx, requests...)
	}

	return nil, nil
}

func (m *mockBackend) ListLedgers(ctx context.Context) (dal.Cursor[*commonpb.LedgerInfo], error) {
	if m.listLedgersFn != nil {
		return m.listLedgersFn(ctx)
	}

	return dal.NewSliceCursor[*commonpb.LedgerInfo](nil), nil
}

func (m *mockBackend) GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error) {
	if m.getLedgerByNameFn != nil {
		return m.getLedgerByNameFn(ctx, name)
	}

	return nil, nil
}

func (m *mockBackend) GetTransaction(ctx context.Context, ledgerName string, txID uint64) (*commonpb.Transaction, error) {
	if m.getTransactionFn != nil {
		return m.getTransactionFn(ctx, ledgerName, txID)
	}

	return nil, nil
}

func (m *mockBackend) ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, reverse bool) (dal.Cursor[*commonpb.Transaction], error) {
	if m.listTransactionsFn != nil {
		return m.listTransactionsFn(ctx, ledgerName, pageSize, afterTxID, filter, reverse)
	}

	return dal.NewSliceCursor[*commonpb.Transaction](nil), nil
}

func (m *mockBackend) GetAccount(ctx context.Context, ledgerName string, address string) (*commonpb.Account, error) {
	if m.getAccountFn != nil {
		return m.getAccountFn(ctx, ledgerName, address)
	}

	return nil, nil
}

func (m *mockBackend) ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter, reverse bool) (dal.Cursor[*commonpb.Account], error) {
	if m.listAccountsFn != nil {
		return m.listAccountsFn(ctx, ledgerName, pageSize, afterAddress, filter, reverse)
	}

	return dal.NewSliceCursor[*commonpb.Account](nil), nil
}

func (m *mockBackend) ListLogs(ctx context.Context, afterSequence uint64, pageSize uint32, filter *commonpb.QueryFilter) (dal.Cursor[*commonpb.Log], error) {
	if m.listLogsFn != nil {
		return m.listLogsFn(ctx, afterSequence, pageSize, filter)
	}

	return dal.NewSliceCursor[*commonpb.Log](nil), nil
}

func (m *mockBackend) GetLog(ctx context.Context, sequence uint64) (*commonpb.Log, error) {
	if m.getLogFn != nil {
		return m.getLogFn(ctx, sequence)
	}

	return nil, nil
}

func (m *mockBackend) ListAuditEntries(ctx context.Context, afterSequence *uint64, failuresOnly bool, pageSize uint32) (dal.Cursor[*auditpb.AuditEntry], error) {
	if m.listAuditEntriesFn != nil {
		return m.listAuditEntriesFn(ctx, afterSequence, failuresOnly, pageSize)
	}

	return dal.NewSliceCursor[*auditpb.AuditEntry](nil), nil
}

func (m *mockBackend) GetAuditEntry(ctx context.Context, sequence uint64) (*auditpb.AuditEntry, error) {
	if m.getAuditEntryFn != nil {
		return m.getAuditEntryFn(ctx, sequence)
	}

	return nil, nil
}

func (m *mockBackend) ListPeriods(ctx context.Context) (dal.Cursor[*commonpb.Period], error) {
	if m.listPeriodsFn != nil {
		return m.listPeriodsFn(ctx)
	}

	return dal.NewSliceCursor[*commonpb.Period](nil), nil
}

func (m *mockBackend) ListSigningKeys(ctx context.Context) (dal.Cursor[*commonpb.SigningKey], error) {
	if m.listSigningKeysFn != nil {
		return m.listSigningKeysFn(ctx)
	}

	return dal.NewSliceCursor[*commonpb.SigningKey](nil), nil
}

func (m *mockBackend) GetMetadataSchemaStatus(ctx context.Context, ledgerName string) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	if m.getMetadataSchemaStatusFn != nil {
		return m.getMetadataSchemaStatusFn(ctx, ledgerName)
	}

	return nil, nil
}

func (m *mockBackend) AnalyzeAccounts(ctx context.Context, ledgerName string, variableThreshold uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
	if m.analyzeAccountsFn != nil {
		return m.analyzeAccountsFn(ctx, ledgerName, variableThreshold)
	}

	return nil, nil
}

func (m *mockBackend) AnalyzeTransactions(ctx context.Context, ledgerName string, variableThreshold uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
	if m.analyzeTransactionsFn != nil {
		return m.analyzeTransactionsFn(ctx, ledgerName, variableThreshold)
	}

	return nil, nil
}

func (m *mockBackend) AggregateVolumes(_ context.Context, _ string, _ *commonpb.QueryFilter) (*commonpb.AggregateResult, error) {
	return &commonpb.AggregateResult{}, nil
}

func (m *mockBackend) ListPreparedQueries(_ context.Context, _ string) ([]*commonpb.PreparedQuery, error) {
	return nil, nil
}

func (m *mockBackend) ExecutePreparedQuery(_ context.Context, _ *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
	return nil, nil
}

func (m *mockBackend) GetLedgerStats(ctx context.Context, ledgerName string) (*commonpb.LedgerStats, error) {
	if m.getLedgerStatsFn != nil {
		return m.getLedgerStatsFn(ctx, ledgerName)
	}

	return &commonpb.LedgerStats{}, nil
}

func (m *mockBackend) GetNumscript(_ context.Context, _ string, _ string) (*commonpb.NumscriptInfo, error) {
	return nil, nil
}

func (m *mockBackend) ListNumscripts(_ context.Context) ([]*commonpb.NumscriptInfo, error) {
	return nil, nil
}

var _ Backend = (*mockBackend)(nil)
