package query

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
)

type Store interface {
	UpdateNextLogID(ctx context.Context, u uint64) error
	IsInitialized() bool
	GetNextLogID(ctx context.Context) (uint64, error)
	ReadLogsRange(ctx context.Context, idMin, idMax uint64) ([]core.PersistedLog, error)
	RunInTransaction(ctx context.Context, f func(ctx context.Context, tx Store) error) error
	GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error)
	GetTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error)
	UpdateAccountsMetadata(ctx context.Context, update []core.Account) error
	InsertTransactions(ctx context.Context, insert ...core.ExpandedTransaction) error
	UpdateTransactionsMetadata(ctx context.Context, update ...core.TransactionWithMetadata) error
	EnsureAccountsExist(ctx context.Context, accounts []string) error
	UpdateVolumes(ctx context.Context, update ...core.AccountsAssetsVolumes) error
}

type defaultStore struct {
	*ledgerstore.Store
}

func (d defaultStore) RunInTransaction(ctx context.Context, f func(ctx context.Context, tx Store) error) error {
	return d.Store.RunInTransaction(ctx, func(ctx context.Context, store *ledgerstore.Store) error {
		return f(ctx, NewDefaultStore(store))
	})
}

var _ Store = (*defaultStore)(nil)

func NewDefaultStore(underlying *ledgerstore.Store) *defaultStore {
	return &defaultStore{
		Store: underlying,
	}
}
