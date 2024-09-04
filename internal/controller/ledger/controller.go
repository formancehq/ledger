package ledger

import (
	"context"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/migrations"
	ledger "github.com/formancehq/ledger/internal"
)

//go:generate mockgen -source controller.go -destination controller_generated.go -package ledger . Controller

type Controller interface {
	GetAccount(ctx context.Context, query GetAccountQuery) (*ledger.ExpandedAccount, error)
	ListAccounts(ctx context.Context, query ListAccountsQuery) (*bunpaginate.Cursor[ledger.ExpandedAccount], error)
	CountAccounts(ctx context.Context, query ListAccountsQuery) (int, error)
	GetAggregatedBalances(ctx context.Context, q GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error)
	GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error)
	Stats(ctx context.Context) (Stats, error)
	ListLogs(ctx context.Context, query GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error)
	CountTransactions(ctx context.Context, query ListTransactionsQuery) (int, error)
	ListTransactions(ctx context.Context, query ListTransactionsQuery) (*bunpaginate.Cursor[ledger.ExpandedTransaction], error)
	GetTransaction(ctx context.Context, query GetTransactionQuery) (*ledger.ExpandedTransaction, error)

	// todo: trace errors and report on interfaces
	// CreateTransaction accept a numscript script and returns a CreatedTransaction
	// It can return following errors:
	//  * ErrCompilationFailed
	//  * ErrMetadataOverride
	//  * ErrInvalidVars
	//  * ErrReferenceConflict
	//  * ErrIdempotencyKeyConflict
	//  * ErrInsufficientFunds
	CreateTransaction(ctx context.Context, parameters Parameters, data ledger.RunScript) (*ledger.Transaction, error)
	RevertTransaction(ctx context.Context, parameters Parameters, id int, force, atEffectiveDate bool) (*ledger.Transaction, error)
	SaveTransactionMetadata(ctx context.Context, parameters Parameters, id int, m metadata.Metadata) error
	SaveAccountMetadata(ctx context.Context, parameters Parameters, id string, m metadata.Metadata) error
	DeleteTransactionMetadata(ctx context.Context, parameters Parameters, id int, key string) error
	DeleteAccountMetadata(ctx context.Context, parameters Parameters, targetID string, key string) error
	Import(ctx context.Context, stream chan ledger.Log) error
	Export(ctx context.Context, w ExportWriter) error

	IsDatabaseUpToDate(ctx context.Context) (bool, error)

	GetVolumesWithBalances(ctx context.Context, q GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error)
}
