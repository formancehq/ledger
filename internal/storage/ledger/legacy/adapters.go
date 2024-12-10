package legacy

import (
	"context"
	"database/sql"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/uptrace/bun"
)

type DefaultStoreAdapter struct {
	newStore       *ledgerstore.Store
	legacyStore    *Store
	isFullUpToDate bool
}

// todo; handle compat with v1
func (d *DefaultStoreAdapter) Accounts() ledgercontroller.PaginatedResource[ledger.Account, any, ledgercontroller.OffsetPaginatedQuery[any]] {
	return d.newStore.Accounts()
}

func (d *DefaultStoreAdapter) Logs() ledgercontroller.PaginatedResource[ledger.Log, any, ledgercontroller.ColumnPaginatedQuery[any]] {
	return d.newStore.Logs()
}

func (d *DefaultStoreAdapter) Transactions() ledgercontroller.PaginatedResource[ledger.Transaction, any, ledgercontroller.ColumnPaginatedQuery[any]] {
	return d.newStore.Transactions()
}

func (d *DefaultStoreAdapter) AggregatedBalances() ledgercontroller.Resource[ledger.AggregatedVolumes, ledgercontroller.GetAggregatedVolumesOptions] {
	return d.newStore.AggregatedVolumes()
}

func (d *DefaultStoreAdapter) Volumes() ledgercontroller.PaginatedResource[ledger.VolumesWithBalanceByAssetByAccount, ledgercontroller.GetVolumesOptions, ledgercontroller.OffsetPaginatedQuery[ledgercontroller.GetVolumesOptions]] {
	return d.newStore.Volumes()
}

func (d *DefaultStoreAdapter) GetDB() bun.IDB {
	return d.newStore.GetDB()
}

func (d *DefaultStoreAdapter) GetBalances(ctx context.Context, query ledgercontroller.BalanceQuery) (ledgercontroller.Balances, error) {
	return d.newStore.GetBalances(ctx, query)
}

func (d *DefaultStoreAdapter) CommitTransaction(ctx context.Context, transaction *ledger.Transaction) error {
	return d.newStore.CommitTransaction(ctx, transaction)
}

func (d *DefaultStoreAdapter) RevertTransaction(ctx context.Context, id int, at time.Time) (*ledger.Transaction, bool, error) {
	return d.newStore.RevertTransaction(ctx, id, at)
}

func (d *DefaultStoreAdapter) UpdateTransactionMetadata(ctx context.Context, transactionID int, m metadata.Metadata) (*ledger.Transaction, bool, error) {
	return d.newStore.UpdateTransactionMetadata(ctx, transactionID, m)
}

func (d *DefaultStoreAdapter) DeleteTransactionMetadata(ctx context.Context, transactionID int, key string) (*ledger.Transaction, bool, error) {
	return d.newStore.DeleteTransactionMetadata(ctx, transactionID, key)
}

func (d *DefaultStoreAdapter) UpdateAccountsMetadata(ctx context.Context, m map[string]metadata.Metadata) error {
	return d.newStore.UpdateAccountsMetadata(ctx, m)
}

func (d *DefaultStoreAdapter) UpsertAccounts(ctx context.Context, accounts ...*ledger.Account) error {
	return d.newStore.UpsertAccounts(ctx, accounts...)
}

func (d *DefaultStoreAdapter) DeleteAccountMetadata(ctx context.Context, address, key string) error {
	return d.newStore.DeleteAccountMetadata(ctx, address, key)
}

func (d *DefaultStoreAdapter) InsertLog(ctx context.Context, log *ledger.Log) error {
	return d.newStore.InsertLog(ctx, log)
}

func (d *DefaultStoreAdapter) LockLedger(ctx context.Context) error {
	return d.newStore.LockLedger(ctx)
}

func (d *DefaultStoreAdapter) ReadLogWithIdempotencyKey(ctx context.Context, ik string) (*ledger.Log, error) {
	return d.newStore.ReadLogWithIdempotencyKey(ctx, ik)
}

func (d *DefaultStoreAdapter) IsUpToDate(ctx context.Context) (bool, error) {
	return d.newStore.HasMinimalVersion(ctx)
}

func (d *DefaultStoreAdapter) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return d.newStore.GetMigrationsInfo(ctx)
}

func (d *DefaultStoreAdapter) BeginTX(ctx context.Context, opts *sql.TxOptions) (ledgercontroller.Store, error) {
	store, err := d.newStore.BeginTX(ctx, opts)
	if err != nil {
		return nil, err
	}

	legacyStore := d.legacyStore.WithDB(store.GetDB())

	return &DefaultStoreAdapter{
		newStore:    store,
		legacyStore: legacyStore,
	}, nil
}

func (d *DefaultStoreAdapter) Commit() error {
	return d.newStore.Commit()
}

func (d *DefaultStoreAdapter) Rollback() error {
	return d.newStore.Rollback()
}

func NewDefaultStoreAdapter(isFullUpToDate bool, store *ledgerstore.Store) *DefaultStoreAdapter {
	return &DefaultStoreAdapter{
		isFullUpToDate: isFullUpToDate,
		newStore:       store,
		legacyStore:    New(store.GetDB(), store.GetLedger().Bucket, store.GetLedger().Name),
	}
}

var _ ledgercontroller.Store = (*DefaultStoreAdapter)(nil)
