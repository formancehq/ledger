package ledger

import (
	"context"
	"database/sql"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/ledger/internal/machine/vm"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/migrations"
	ledger "github.com/formancehq/ledger/internal"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source controller.go -destination controller_generated_test.go -package ledger . Controller

type Controller interface {
	BeginTX(ctx context.Context, options *sql.TxOptions) (Controller, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error

	// IsDatabaseUpToDate check if the ledger store is up to date, including the bucket and the ledger specifics
	// It returns true if up to date
	IsDatabaseUpToDate(ctx context.Context) (bool, error)
	GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error)
	GetStats(ctx context.Context) (Stats, error)

	GetAccount(ctx context.Context, query ResourceQuery[any]) (*ledger.Account, error)
	ListAccounts(ctx context.Context, query OffsetPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Account], error)
	CountAccounts(ctx context.Context, query ResourceQuery[any]) (int, error)
	ListLogs(ctx context.Context, query ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error)
	CountTransactions(ctx context.Context, query ResourceQuery[any]) (int, error)
	ListTransactions(ctx context.Context, query ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Transaction], error)
	GetTransaction(ctx context.Context, query ResourceQuery[any]) (*ledger.Transaction, error)
	GetVolumesWithBalances(ctx context.Context, q OffsetPaginatedQuery[GetVolumesOptions]) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error)
	GetAggregatedBalances(ctx context.Context, q ResourceQuery[GetAggregatedVolumesOptions]) (ledger.BalancesByAssets, error)

	// CreateTransaction accept a numscript script and returns a transaction
	// It can return following errors:
	//  * ErrCompilationFailed
	//  * ErrMetadataOverride
	//  * ErrInvalidVars
	//  * ErrTransactionReferenceConflict
	//  * ErrIdempotencyKeyConflict
	//  * ErrInsufficientFunds
	CreateTransaction(ctx context.Context, parameters Parameters[RunScript]) (*ledger.Log, *ledger.CreatedTransaction, error)
	// RevertTransaction allow to revert a transaction.
	// It can return following errors:
	//  * ErrInsufficientFunds
	//  * ErrAlreadyReverted
	//  * ErrNotFound
	// Parameter force indicate we want to force revert the transaction even if the accounts does not have funds
	// Parameter atEffectiveDate indicate we want to set the timestamp of the newly created transaction on the timestamp of the reverted transaction
	RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error)
	// SaveTransactionMetadata allow to add metadata to an existing transaction
	// It can return following errors:
	//  * ErrNotFound
	SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error)
	// SaveAccountMetadata allow to add metadata to an account
	// If the account does not exist, it is created
	SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error)
	// DeleteTransactionMetadata allow to remove metadata of a transaction
	// It can return following errors:
	//  * ErrNotFound : indicate the transaction was not found OR the metadata does not exist on the transaction
	DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error)
	// DeleteAccountMetadata allow to remove metadata of an account
	// It can return following errors:
	//  * ErrNotFound : indicate the account was not found OR the metadata does not exist on the account
	DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error)
	// Import allow to import the logs of an existing ledger
	// It can return following errors:
	//  * ErrImport
	// Logs hash must be valid and the ledger.Ledger must be in 'initializing' state
	Import(ctx context.Context, stream chan ledger.Log) error
	// Export allow to export the logs of a ledger
	Export(ctx context.Context, w ExportWriter) error
}

type RunScript = vm.RunScript
type Script = vm.Script
type ScriptV1 = vm.ScriptV1

type RevertTransaction struct {
	Force           bool
	AtEffectiveDate bool
	TransactionID   int
}

type SaveTransactionMetadata struct {
	TransactionID int
	Metadata      metadata.Metadata
}

type SaveAccountMetadata struct {
	Address  string
	Metadata metadata.Metadata
}

type DeleteTransactionMetadata struct {
	TransactionID int
	Key           string
}

type DeleteAccountMetadata struct {
	Address string
	Key     string
}
