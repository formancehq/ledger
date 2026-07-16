package ctrl

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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
	ListLedgers(ctx context.Context) (cursor.Cursor[*commonpb.LedgerInfo], error)
	GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error)

	// Read operations
	// GetTransaction returns the transaction and the receipt the serving node
	// signed. On a locally-served read the receipt is signed by the controller
	// from a snapshot opened after the transaction read (its freshness barrier):
	// non-nil when a signer is configured — possibly an empty string, e.g. a
	// reversal — and nil when the node has no signer. When the read is forwarded
	// to a signing node the returned receipt is that node's authoritative token
	// (again possibly empty), relayed as-is rather than recomputed against a
	// possibly-stale local snapshot. Adapters surface the token verbatim and must
	// not re-sign it.
	GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, *string, error)
	ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*commonpb.Transaction], error)
	GetAccount(ctx context.Context, ledgerName string, address string, opts GetAccountOptions) (*commonpb.Account, error)
	ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*commonpb.Account], error)

	// Stats operations
	GetLedgerStats(ctx context.Context, ledgerName string) (*commonpb.LedgerStats, error)

	// Log operations
	// ListLogs returns logs for a specific ledger, ordered by ledger-local log
	// ID. afterSequence is the ledger-local log ID to start after; the filter
	// may add further conditions (e.g. date ranges). Use a LogIdCondition in
	// the filter for pagination.
	ListLogs(ctx context.Context, ledgerName string, afterSequence uint64, pageSize uint32, filter *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error)
	GetLog(ctx context.Context, sequence uint64) (*commonpb.Log, error)

	// Audit operations
	ListAuditEntries(ctx context.Context, pageSize uint32, afterSequence uint64, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*auditpb.AuditEntry], error)
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

	// GetTemplateUsage returns the invocation counter and last-used timestamp
	// for a Numscript template. Reads from the usagebuilder side-store, so
	// values may lag the live FSM by up to one tick interval. Returns a
	// zero-valued TemplateUsage when the template has never been invoked (or
	// the usagebuilder has not caught up to any of its invocations yet).
	GetTemplateUsage(ctx context.Context, ledger, name string) (*commonpb.TemplateUsage, error)

	// Cluster-wide config operations (read-only)
	GetChapterSchedule(ctx context.Context) (string, error)
	GetEventsSinks(ctx context.Context) ([]*commonpb.SinkConfig, []*commonpb.SinkStatus, error)

	// Index inspection
	InspectIndex(ctx context.Context, req *servicepb.InspectIndexRequest) (*servicepb.InspectIndexResponse, error)

	// Index registry — ListIndexes streams the bucket-scoped index registry
	// (optionally filtered by ledger via req.Scope/req.Ledger),
	// GetIndexStatus returns the aggregated (per-index cursor + per-replica
	// version) snapshot exposed on the registry, and the single-entry
	// getters return the Index / IndexEntry for a given (ledger, id) tuple.
	ListIndexes(ctx context.Context, req *servicepb.ListIndexesRequest) (cursor.Cursor[*commonpb.Index], error)
	GetIndexStatus(ctx context.Context, req *servicepb.GetIndexStatusRequest) (*servicepb.GetIndexStatusResponse, error)
	GetIndex(ctx context.Context, req *servicepb.GetIndexRequest) (*commonpb.Index, error)
	GetIndexEntryStatus(ctx context.Context, req *servicepb.GetIndexEntryStatusRequest) (*servicepb.IndexEntry, error)

	// Write operations - single entry point for all requests. The ApplyRequest
	// is one atomic batch, signed or unsigned at the batch level.
	Apply(ctx context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error)

	// Barrier proposes a no-op through Raft consensus. When it returns, all
	// previously proposed entries are guaranteed to have been applied.
	// Returns the Raft commit index at which the barrier was applied.
	Barrier(ctx context.Context) (uint64, error)
}
