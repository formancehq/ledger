package storage

import (
	"context"
	"errors"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/stack/libs/go-libs/api"
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

	GetNextLogID(ctx context.Context) (uint64, error)
	ReadLogsStartingFromID(ctx context.Context, id uint64) ([]core.Log, error)
	UpdateNextLogID(ctx context.Context, id uint64) error
	InsertTransactions(ctx context.Context, transaction ...core.ExpandedTransaction) error
	UpdateAccountMetadata(ctx context.Context, id string, metadata core.Metadata) error
	UpdateTransactionMetadata(ctx context.Context, id uint64, metadata core.Metadata) error
	GetAccountWithVolumes(ctx context.Context, addr string) (*core.AccountWithVolumes, error)
	UpdateVolumes(ctx context.Context, volumes core.AccountsAssetsVolumes) error
	EnsureAccountExists(ctx context.Context, account string) error
	GetLastTransaction(ctx context.Context) (*core.ExpandedTransaction, error)
	CountTransactions(context.Context, TransactionsQuery) (uint64, error)
	GetTransactions(context.Context, TransactionsQuery) (api.Cursor[core.ExpandedTransaction], error)
	GetTransaction(ctx context.Context, txid uint64) (*core.ExpandedTransaction, error)
	GetAccount(ctx context.Context, accountAddress string) (*core.Account, error)
	GetAssetsVolumes(ctx context.Context, accountAddress string) (core.AssetsVolumes, error)
	CountAccounts(context.Context, AccountsQuery) (uint64, error)
	GetAccounts(context.Context, AccountsQuery) (api.Cursor[core.Account], error)
	GetBalances(context.Context, BalancesQuery) (api.Cursor[core.AccountsBalances], error)
	GetBalancesAggregated(context.Context, BalancesQuery) (core.AssetsBalances, error)
	GetLastLog(context.Context) (*core.Log, error)
	GetLogs(context.Context, *LogsQuery) (api.Cursor[core.Log], error)
	AppendLog(context.Context, *core.Log) error
	GetMigrationsAvailable() ([]core.MigrationInfo, error)
	GetMigrationsDone(context.Context) ([]core.MigrationInfo, error)
	ReadLogWithReference(ctx context.Context, reference string) (*core.Log, error)
	ReadLastLogWithType(ctx context.Context, logType ...core.LogType) (*core.Log, error)
}

type Driver interface {
	GetSystemStore() SystemStore
	GetLedgerStore(ctx context.Context, name string, create bool) (LedgerStore, bool, error)
	Initialize(ctx context.Context) error
	Close(ctx context.Context) error
	Name() string
}
