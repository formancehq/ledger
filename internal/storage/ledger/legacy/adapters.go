package legacy

import (
	"context"
	"database/sql"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/uptrace/bun"
)

type TX struct {
	newStore    *ledgerstore.Store
	legacyStore *Store
	sqlTX       bun.Tx
}

func (tx TX) GetAccount(ctx context.Context, query ledgercontroller.GetAccountQuery) (*ledger.Account, error) {
	return tx.legacyStore.GetAccountWithVolumes(ctx, query)
}

func (tx TX) GetBalances(ctx context.Context, query ledgercontroller.BalanceQuery) (ledgercontroller.Balances, error) {
	return tx.newStore.GetBalances(ctx, query)
}

func (tx TX) CommitTransaction(ctx context.Context, transaction *ledger.Transaction) error {
	return tx.newStore.CommitTransaction(ctx, transaction)
}

func (tx TX) RevertTransaction(ctx context.Context, id int, at time.Time) (*ledger.Transaction, bool, error) {
	return tx.newStore.RevertTransaction(ctx, id, at)
}

func (tx TX) UpdateTransactionMetadata(ctx context.Context, transactionID int, m metadata.Metadata) (*ledger.Transaction, bool, error) {
	return tx.newStore.UpdateTransactionMetadata(ctx, transactionID, m)
}

func (tx TX) DeleteTransactionMetadata(ctx context.Context, transactionID int, key string) (*ledger.Transaction, bool, error) {
	return tx.newStore.DeleteTransactionMetadata(ctx, transactionID, key)
}

func (tx TX) UpdateAccountsMetadata(ctx context.Context, m map[string]metadata.Metadata) error {
	return tx.newStore.UpdateAccountsMetadata(ctx, m)
}

func (tx TX) UpsertAccount(ctx context.Context, account *ledger.Account) (bool, error) {
	return tx.newStore.UpsertAccount(ctx, account)
}

func (tx TX) DeleteAccountMetadata(ctx context.Context, address, key string) error {
	return tx.newStore.DeleteAccountMetadata(ctx, address, key)
}

func (tx TX) InsertLog(ctx context.Context, log *ledger.Log) error {
	return tx.newStore.InsertLog(ctx, log)
}

func (tx TX) LockLedger(ctx context.Context) error {
	return tx.newStore.LockLedger(ctx)
}

func (tx TX) ListLogs(ctx context.Context, q ledgercontroller.GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error) {
	return tx.legacyStore.GetLogs(ctx, q)
}

type DefaultStoreAdapter struct {
	newStore    *ledgerstore.Store
	legacyStore *Store
}

func (d *DefaultStoreAdapter) GetDB() bun.IDB {
	return d.newStore.GetDB()
}

func (d *DefaultStoreAdapter) ListLogs(ctx context.Context, q ledgercontroller.GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error) {
	return d.legacyStore.GetLogs(ctx, q)
}

func (d *DefaultStoreAdapter) ReadLogWithIdempotencyKey(ctx context.Context, ik string) (*ledger.Log, error) {
	return d.newStore.ReadLogWithIdempotencyKey(ctx, ik)
}

func (d *DefaultStoreAdapter) ListTransactions(ctx context.Context, q ledgercontroller.ListTransactionsQuery) (*bunpaginate.Cursor[ledger.Transaction], error) {
	return d.legacyStore.GetTransactions(ctx, q)
}

func (d *DefaultStoreAdapter) CountTransactions(ctx context.Context, q ledgercontroller.ListTransactionsQuery) (int, error) {
	return d.legacyStore.CountTransactions(ctx, q)
}

func (d *DefaultStoreAdapter) GetTransaction(ctx context.Context, query ledgercontroller.GetTransactionQuery) (*ledger.Transaction, error) {
	return d.legacyStore.GetTransactionWithVolumes(ctx, query)
}

func (d *DefaultStoreAdapter) CountAccounts(ctx context.Context, q ledgercontroller.ListAccountsQuery) (int, error) {
	return d.legacyStore.CountAccounts(ctx, q)
}

func (d *DefaultStoreAdapter) ListAccounts(ctx context.Context, q ledgercontroller.ListAccountsQuery) (*bunpaginate.Cursor[ledger.Account], error) {
	return d.legacyStore.GetAccountsWithVolumes(ctx, q)
}

func (d *DefaultStoreAdapter) GetAccount(ctx context.Context, q ledgercontroller.GetAccountQuery) (*ledger.Account, error) {
	return d.legacyStore.GetAccountWithVolumes(ctx, q)
}

func (d *DefaultStoreAdapter) GetAggregatedBalances(ctx context.Context, q ledgercontroller.GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {
	return d.legacyStore.GetAggregatedBalances(ctx, q)
}

func (d *DefaultStoreAdapter) GetVolumesWithBalances(ctx context.Context, q ledgercontroller.GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	return d.legacyStore.GetVolumesWithBalances(ctx, q)
}

func (d *DefaultStoreAdapter) IsUpToDate(ctx context.Context) (bool, error) {
	return d.newStore.HasMinimalVersion(ctx)
}

func (d *DefaultStoreAdapter) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return d.newStore.GetMigrationsInfo(ctx)
}

func (d *DefaultStoreAdapter) WithTX(ctx context.Context, opts *sql.TxOptions, f func(ledgercontroller.TX) (bool, error)) error {
	if opts == nil {
		opts = &sql.TxOptions{}
	}

	tx, err := d.newStore.GetDB().BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if commit, err := f(&TX{
		newStore:    d.newStore.WithDB(tx),
		legacyStore: d.legacyStore.WithDB(tx),
		sqlTX:       tx,
	}); err != nil {
		return err
	} else if commit {
		return tx.Commit()
	}

	return nil
}

func NewDefaultStoreAdapter(store *ledgerstore.Store) *DefaultStoreAdapter {
	return &DefaultStoreAdapter{
		newStore:    store,
		legacyStore: New(store.GetDB(), store.GetLedger().Bucket, store.GetLedger().Name),
	}
}

var _ ledgercontroller.Store = (*DefaultStoreAdapter)(nil)
