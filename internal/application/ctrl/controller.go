package ctrl

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source controller.go -destination controller_generated_test.go -package ctrl . Controller
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source controller.go -destination ctrlmock/controller_generated.go -package ctrlmock . Controller
type Controller interface {
	// Ledger management (read-only)
	ListLedgers(ctx context.Context) (cursor.Cursor[*commonpb.LedgerInfo], error)
	GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error)

	// Read operations
	GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error)
	ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*commonpb.Transaction], error)
	GetAccount(ctx context.Context, ledgerName string, address string) (*commonpb.Account, error)
	ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*commonpb.Account], error)

	// Stats operations
	GetLedgerStats(ctx context.Context, ledgerName string) (*commonpb.LedgerStats, error)

	// Log operations
	// ListLogs returns logs for a specific ledger, ordered by ledger-local log ID.
	// Use a LogIdCondition in the filter for pagination.
	ListLogs(ctx context.Context, ledgerName string, afterSequence uint64, pageSize uint32, filter *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error)
	GetLog(ctx context.Context, sequence uint64) (*commonpb.Log, error)

	// Audit operations
	ListAuditEntries(ctx context.Context, afterSequence *uint64, failuresOnly bool, pageSize uint32, ledger string) (cursor.Cursor[*auditpb.AuditEntry], error)
	GetAuditEntry(ctx context.Context, sequence uint64) (*auditpb.AuditEntry, error)

	// Chapter operations
	ListChapters(ctx context.Context) (cursor.Cursor[*commonpb.Chapter], error)

	// Signing key operations
	ListSigningKeys(ctx context.Context) (cursor.Cursor[*commonpb.SigningKey], error)

	// Schema operations
	GetMetadataSchemaStatus(ctx context.Context, ledgerName string) (*servicepb.GetMetadataSchemaStatusResponse, error)

	// Analysis operations
	AnalyzeAccounts(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*servicepb.AnalyzeAccountsResponse, error)
	AnalyzeTransactions(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*servicepb.AnalyzeTransactionsResponse, error)

	// Aggregation operations
	AggregateVolumes(ctx context.Context, ledgerName string, filter *commonpb.QueryFilter, opts query.AggregateOptions) (*commonpb.AggregateResult, error)

	// Prepared query operations (read-only)
	ListPreparedQueries(ctx context.Context, ledger string) ([]*commonpb.PreparedQuery, error)
	ExecutePreparedQuery(ctx context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error)

	// Numscript library operations
	GetNumscript(ctx context.Context, ledger, name string, version string) (*commonpb.NumscriptInfo, error)
	ListNumscripts(ctx context.Context, ledger string) ([]*commonpb.NumscriptInfo, error)

	// Cluster-wide config operations (read-only)
	GetChapterSchedule(ctx context.Context) (string, error)
	GetEventsSinks(ctx context.Context) ([]*commonpb.SinkConfig, error)

	// Index inspection
	InspectIndex(ctx context.Context, req *servicepb.InspectIndexRequest) (*servicepb.InspectIndexResponse, error)

	// Write operations - single entry point for all requests
	Apply(ctx context.Context, envelopes ...*servicepb.Envelope) ([]*commonpb.Log, error)

	// Barrier proposes a no-op through Raft consensus. When it returns, all
	// previously proposed entries are guaranteed to have been applied.
	// Returns the Raft commit index at which the barrier was applied.
	Barrier(ctx context.Context) (uint64, error)
}
