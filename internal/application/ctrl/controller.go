package ctrl

import (
	"context"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/query"
)

// GetAccountOptions configures a GetAccount read.
type GetAccountOptions struct {
	// CollapseColors sums every colored bucket of the same asset into a
	// single entry with Color = "" in the returned Account.volumes list.
	// When false (default), each (asset, color) tuple gets its own entry.
	CollapseColors bool
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source controller.go -destination controller_generated_test.go -package ctrl . Controller
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source controller.go -destination ctrlmock/controller_generated.go -package ctrlmock . Controller
type Controller interface {
	// Ledger management (read-only)
	ListLedgers(ctx context.Context) (cursor.Cursor[*ledgerpb.LedgerInfo], error)
	GetLedgerByName(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error)

	// Read operations
	GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*ledgerpb.Transaction, error)
	ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64, filter *ledgerpb.QueryFilter, reverse bool) (cursor.Cursor[*ledgerpb.Transaction], error)
	GetAccount(ctx context.Context, ledgerName string, address string, opts GetAccountOptions) (*ledgerpb.Account, error)
	ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, filter *ledgerpb.QueryFilter, reverse bool) (cursor.Cursor[*ledgerpb.Account], error)

	// Stats operations
	GetLedgerStats(ctx context.Context, ledgerName string) (*ledgerpb.LedgerStats, error)

	// Log operations
	// ListLogs returns logs for a specific ledger, ordered by ledger-local log
	// ID. afterSequence is the ledger-local log ID to start after; the filter
	// may add further conditions (e.g. date ranges). Use a LogIdCondition in
	// the filter for pagination.
	ListLogs(ctx context.Context, ledgerName string, afterSequence uint64, pageSize uint32, filter *ledgerpb.QueryFilter) (cursor.Cursor[*ledgerpb.Log], error)
	GetLog(ctx context.Context, sequence uint64) (*ledgerpb.Log, error)

	// Audit operations
	ListAuditEntries(ctx context.Context, pageSize uint32, afterSequence uint64, filter *ledgerpb.QueryFilter, reverse bool) (cursor.Cursor[*ledgerpb.AuditEntry], error)
	GetAuditEntry(ctx context.Context, sequence uint64) (*ledgerpb.AuditEntry, error)

	// Signing key operations
	ListSigningKeys(ctx context.Context) (cursor.Cursor[*ledgerpb.SigningKey], error)

	// Schema operations
	GetMetadataSchemaStatus(ctx context.Context, ledgerName string) (*ledgerpb.GetMetadataSchemaStatusResponse, error)

	// Analysis operations
	AnalyzeAccounts(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*ledgerpb.AnalyzeAccountsResponse, error)
	AnalyzeTransactions(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*ledgerpb.AnalyzeTransactionsResponse, error)

	// Aggregation operations
	AggregateVolumes(ctx context.Context, ledgerName string, filter *ledgerpb.QueryFilter, opts query.AggregateOptions) (*ledgerpb.AggregateResult, error)

	// Prepared query operations (read-only)
	ListPreparedQueries(ctx context.Context, ledger string) ([]*ledgerpb.PreparedQuery, error)
	ExecutePreparedQuery(ctx context.Context, req *ledgerpb.ExecutePreparedQueryRequest) (*ledgerpb.ExecutePreparedQueryResponse, error)

	// Numscript library operations
	GetNumscript(ctx context.Context, ledger, name string, version string) (*ledgerpb.NumscriptInfo, error)
	ListNumscripts(ctx context.Context, ledger string) ([]*ledgerpb.NumscriptInfo, error)
	ListNumscriptVersions(ctx context.Context, ledger, name string) (string, []*ledgerpb.NumscriptVersionEntry, error)

	// GetTemplateUsage returns the invocation counter and last-used timestamp
	// for a Numscript template. Reads from the usagebuilder side-store, so
	// values may lag the live FSM while the worker drains pending batches.
	// Returns a zero-valued TemplateUsage when the template has never been
	// invoked (or the usagebuilder has not caught up to any invocation yet).
	GetTemplateUsage(ctx context.Context, ledger, name string) (*ledgerpb.TemplateUsage, error)

	// Cluster-wide config operations (read-only)
	GetEventsSinks(ctx context.Context) ([]*ledgerpb.SinkConfig, []*ledgerpb.SinkStatus, error)

	// Index inspection
	InspectIndex(ctx context.Context, req *ledgerpb.InspectIndexRequest) (*ledgerpb.InspectIndexResponse, error)

	// Index registry — ListIndexes streams the bucket-scoped index registry
	// (optionally filtered by ledger via req.Scope/req.Ledger),
	// GetIndexStatus returns the aggregated (per-index cursor + per-replica
	// version) snapshot exposed on the registry, and the single-entry
	// getters return the Index / IndexEntry for a given (ledger, id) tuple.
	ListIndexes(ctx context.Context, req *ledgerpb.ListIndexesRequest) (cursor.Cursor[*ledgerpb.Index], error)
	GetIndexStatus(ctx context.Context, req *ledgerpb.GetIndexStatusRequest) (*ledgerpb.GetIndexStatusResponse, error)
	GetIndex(ctx context.Context, req *ledgerpb.GetIndexRequest) (*ledgerpb.Index, error)
	GetIndexEntryStatus(ctx context.Context, req *ledgerpb.GetIndexEntryStatusRequest) (*ledgerpb.IndexEntry, error)

	// Write operations - single entry point for all requests. The ApplyRequest
	// is one atomic batch, signed or unsigned at the batch level.
	Apply(ctx context.Context, req *ledgerpb.ApplyRequest) (*domain.ApplyResult, error)

	// Barrier proposes a no-op through Raft consensus. When it returns, all
	// previously proposed entries are guaranteed to have been applied.
	// Returns the Raft commit index at which the barrier was applied.
	Barrier(ctx context.Context) (uint64, error)
}
