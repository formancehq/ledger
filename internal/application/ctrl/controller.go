package ctrl

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source controller.go -destination controller_generated_test.go -package ctrl . Controller
type Controller interface {
	// Ledger management (read-only)
	ListLedgers(ctx context.Context) (dal.Cursor[*commonpb.LedgerInfo], error)
	GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error)

	// Read operations
	GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error)
	ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64) (dal.Cursor[*commonpb.Transaction], error)
	GetAccount(ctx context.Context, ledgerName string, address string) (*commonpb.Account, error)
	ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, prefix string) (dal.Cursor[*commonpb.Account], error)

	// Log operations
	ListLogs(ctx context.Context, afterSequence uint64, pageSize uint32) (dal.Cursor[*commonpb.Log], error)
	GetLog(ctx context.Context, sequence uint64) (*commonpb.Log, error)

	// Audit operations
	ListAuditEntries(ctx context.Context, afterSequence *uint64, failuresOnly bool, pageSize uint32) (dal.Cursor[*auditpb.AuditEntry], error)
	GetAuditEntry(ctx context.Context, sequence uint64) (*auditpb.AuditEntry, error)

	// Period operations
	ListPeriods(ctx context.Context) (dal.Cursor[*commonpb.Period], error)

	// Signing key operations
	ListSigningKeys(ctx context.Context) (dal.Cursor[*commonpb.SigningKey], error)

	// Schema operations
	GetMetadataSchemaStatus(ctx context.Context, ledgerName string) (*servicepb.GetMetadataSchemaStatusResponse, error)

	// Analysis operations
	AnalyzeAccounts(ctx context.Context, ledgerName string, variableThreshold uint32) (*servicepb.AnalyzeAccountsResponse, error)

	// Write operations - single entry point for all requests
	Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error)
}
