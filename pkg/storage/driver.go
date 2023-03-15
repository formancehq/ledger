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
	Name() string

	GetLastTransaction(ctx context.Context) (*core.ExpandedTransaction, error)
	CountTransactions(context.Context, TransactionsQuery) (uint64, error)
	GetTransactions(context.Context, TransactionsQuery) (api.Cursor[core.ExpandedTransaction], error)
	GetTransaction(ctx context.Context, txid uint64) (*core.ExpandedTransaction, error)
	GetAccount(ctx context.Context, accountAddress string) (*core.Account, error)
	GetAssetsVolumes(ctx context.Context, accountAddress string) (core.AssetsVolumes, error)
	GetAccountWithVolumes(ctx context.Context, account string) (*core.AccountWithVolumes, error)
	CountAccounts(context.Context, AccountsQuery) (uint64, error)
	GetAccounts(context.Context, AccountsQuery) (api.Cursor[core.Account], error)
	GetBalances(context.Context, BalancesQuery) (api.Cursor[core.AccountsBalances], error)
	GetBalancesAggregated(context.Context, BalancesQuery) (core.AssetsBalances, error)
	GetLastLog(context.Context) (*core.Log, error)
	GetLogs(context.Context, *LogsQuery) (api.Cursor[core.Log], error)
	AppendLogs(context.Context, ...core.Log) <-chan error
	GetMigrationsAvailable() ([]core.MigrationInfo, error)
	GetMigrationsDone(context.Context) ([]core.MigrationInfo, error)

	UpdateTransactionMetadata(ctx context.Context, txid uint64, metadata core.Metadata) error
	UpdateAccountMetadata(ctx context.Context, address string, metadata core.Metadata) error
	Commit(ctx context.Context, txs ...core.ExpandedTransaction) error
}

type Driver interface {
	Initialize(ctx context.Context) error
	Close(ctx context.Context) error
	Name() string

	GetSystemStore() SystemStore
	GetLedgerStore(ctx context.Context, name string, create bool) (LedgerStore, bool, error)
}
