package storage

import (
	"context"
	"errors"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

var (
	ErrLedgerStoreNotFound = errors.New("ledger store not found")
)

type SystemStore interface {
	GetConfiguration(ctx context.Context, key string) (string, error)
	InsertConfiguration(ctx context.Context, key, value string) error
	ListLedgers(ctx context.Context) ([]string, error)
	DeleteLedger(ctx context.Context, name string) error
}

type LedgerStore interface {
	Delete(ctx context.Context) error
	Initialize(ctx context.Context) (bool, error)
	Close(ctx context.Context) error
	IsInitialized() bool
	Name() string

	RunInTransaction(ctx context.Context, f func(ctx context.Context, store LedgerStore) error) error

	AppendLog(context.Context, *core.Log) error
	GetNextLogID(ctx context.Context) (uint64, error)
	ReadLogsRange(ctx context.Context, idMin, idMax uint64) ([]core.Log, error)
	UpdateNextLogID(ctx context.Context, id uint64) error
	GetLogs(context.Context, LogsQuery) (*api.Cursor[core.Log], error)
	GetLastLog(context.Context) (*core.Log, error)
	ReadLastLogWithType(ctx context.Context, logType ...core.LogType) (*core.Log, error)
	ReadLogForCreatedTransactionWithReference(ctx context.Context, reference string) (*core.Log, error)
	ReadLogForCreatedTransaction(ctx context.Context, txID uint64) (*core.Log, error)
	ReadLogForRevertedTransaction(ctx context.Context, txID uint64) (*core.Log, error)
	ReadLogWithIdempotencyKey(ctx context.Context, key string) (*core.Log, error)

	InsertTransactions(ctx context.Context, transaction ...core.ExpandedTransaction) error
	UpdateTransactionMetadata(ctx context.Context, id uint64, metadata metadata.Metadata) error
	UpdateTransactionsMetadata(ctx context.Context, txs ...core.TransactionWithMetadata) error
	CountTransactions(context.Context, TransactionsQuery) (uint64, error)
	GetTransactions(context.Context, TransactionsQuery) (*api.Cursor[core.ExpandedTransaction], error)
	GetTransaction(ctx context.Context, txid uint64) (*core.ExpandedTransaction, error)

	UpdateAccountMetadata(ctx context.Context, id string, metadata metadata.Metadata) error
	UpdateAccountsMetadata(ctx context.Context, accounts []core.Account) error
	EnsureAccountExists(ctx context.Context, account string) error
	EnsureAccountsExist(ctx context.Context, accounts []string) error
	CountAccounts(context.Context, AccountsQuery) (uint64, error)
	GetAccounts(context.Context, AccountsQuery) (*api.Cursor[core.Account], error)
	GetAccountWithVolumes(ctx context.Context, addr string) (*core.AccountWithVolumes, error)
	GetAccount(ctx context.Context, accountAddress string) (*core.Account, error)

	UpdateVolumes(ctx context.Context, volumes ...core.AccountsAssetsVolumes) error
	GetAssetsVolumes(ctx context.Context, accountAddress string) (core.AssetsVolumes, error)

	GetBalances(context.Context, BalancesQuery) (*api.Cursor[core.AccountsBalances], error)
	GetBalancesAggregated(context.Context, BalancesQuery) (core.AssetsBalances, error)

	GetMigrationsAvailable() ([]core.MigrationInfo, error)
	GetMigrationsDone(context.Context) ([]core.MigrationInfo, error)
}

type Driver interface {
	GetSystemStore() SystemStore
	GetLedgerStore(ctx context.Context, name string, create bool) (LedgerStore, bool, error)
	Initialize(ctx context.Context) error
	Close(ctx context.Context) error
	Name() string
}
