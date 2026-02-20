package ctrl

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source controller.go -destination controller_generated_test.go -package ctrl . Controller
type Controller interface {
	// Ledger management (read-only)
	GetAllLedgersInfo(ctx context.Context) (data.Cursor[*commonpb.LedgerInfo], error)
	GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error)

	// Read operations
	GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error)
	ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64) (data.Cursor[*commonpb.Transaction], error)
	GetAccount(ctx context.Context, ledgerName string, address string) (*commonpb.Account, error)
	ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, prefix string) (data.Cursor[*commonpb.Account], error)

	// Log operations
	ListLogs(ctx context.Context, afterSequence uint64, ledger string, pageSize uint32) (data.Cursor[*commonpb.Log], error)

	// Period operations
	ListPeriods(ctx context.Context) ([]*commonpb.Period, error)

	// Write operations - single entry point for all requests
	Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error)
}
