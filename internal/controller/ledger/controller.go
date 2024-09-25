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
	// IsDatabaseUpToDate check if the ledger store is up to date, including the bucket and the ledger specifics
	// It returns true if up to date
	IsDatabaseUpToDate(ctx context.Context) (bool, error)
	GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error)
	GetStats(ctx context.Context) (Stats, error)

	GetAccount(ctx context.Context, query GetAccountQuery) (*ledger.ExpandedAccount, error)
	ListAccounts(ctx context.Context, query ListAccountsQuery) (*bunpaginate.Cursor[ledger.ExpandedAccount], error)
	CountAccounts(ctx context.Context, query ListAccountsQuery) (int, error)
	ListLogs(ctx context.Context, query GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error)
	CountTransactions(ctx context.Context, query ListTransactionsQuery) (int, error)
	ListTransactions(ctx context.Context, query ListTransactionsQuery) (*bunpaginate.Cursor[ledger.Transaction], error)
	GetTransaction(ctx context.Context, query GetTransactionQuery) (*ledger.Transaction, error)
	GetVolumesWithBalances(ctx context.Context, q GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error)
	GetAggregatedBalances(ctx context.Context, q GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error)

	// CreateTransaction accept a numscript script and returns a CreatedTransaction
	// It can return following errors:
	//  * ErrCompilationFailed
	//  * ErrMetadataOverride
	//  * ErrInvalidVars
	//  * ErrReferenceConflict
	//  * ErrIdempotencyKeyConflict
	//  * ErrInsufficientFunds
	CreateTransaction(ctx context.Context, parameters Parameters, data ledger.RunScript) (*ledger.Transaction, error)
	// RevertTransaction allow to revert a transaction.
	// It can return following errors:
	//  * ErrInsufficientFunds
	//  * ErrAlreadyReverted
	//  * ErrNotFound
	// Parameter force indicate we want to force revert the transaction even if the accounts does not have funds
	// Parameter atEffectiveDate indicate we want to set the timestamp of the newly created transaction on the timestamp of the reverted transaction
	RevertTransaction(ctx context.Context, parameters Parameters, id int, force, atEffectiveDate bool) (*ledger.Transaction, error)
	// SaveTransactionMetadata allow to add metadata to an existing transaction
	// It can return following errors:
	//  * ErrNotFound
	SaveTransactionMetadata(ctx context.Context, parameters Parameters, id int, m metadata.Metadata) error
	// SaveAccountMetadata allow to add metadata to an account
	// If the account does not exist, it is created
	SaveAccountMetadata(ctx context.Context, parameters Parameters, id string, m metadata.Metadata) error
	// DeleteTransactionMetadata allow to remove metadata of a transaction
	// It can return following errors:
	//  * ErrNotFound : indicate the transaction was not found OR the metadata does not exist on the transaction
	DeleteTransactionMetadata(ctx context.Context, parameters Parameters, id int, key string) error
	// DeleteAccountMetadata allow to remove metadata of an account
	// It can return following errors:
	//  * ErrNotFound : indicate the account was not found OR the metadata does not exist on the account
	DeleteAccountMetadata(ctx context.Context, parameters Parameters, targetID string, key string) error
	// Import allow to import the logs of an existing ledger
	// It can return following errors:
	//  * ErrImport
	// Logs hash must be valid and the ledger.Ledger must be in 'initializing' state
	Import(ctx context.Context, stream chan ledger.Log) error
	// Export allow to export the logs of a ledger
	Export(ctx context.Context, w ExportWriter) error
}
