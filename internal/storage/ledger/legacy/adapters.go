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
	"math/big"
	"slices"
)

type accountsPaginatedResourceAdapter struct {
	store *Store
}

func (p accountsPaginatedResourceAdapter) GetOne(ctx context.Context, query ledgercontroller.ResourceQuery[any]) (*ledger.Account, error) {
	var address string
	_ = query.Builder.Walk(func(_ string, _ string, value any) error {
		address = value.(string)
		return nil
	})
	return p.store.GetAccountWithVolumes(ctx, GetAccountQuery{
		PITFilterWithVolumes: PITFilterWithVolumes{
			PITFilter: PITFilter{
				PIT: query.PIT,
				OOT: query.OOT,
			},
			ExpandVolumes:          slices.Contains(query.Expand, "volumes"),
			ExpandEffectiveVolumes: slices.Contains(query.Expand, "effectiveVolumes"),
		},
		Addr: address,
	})
}

func (p accountsPaginatedResourceAdapter) Count(ctx context.Context, query ledgercontroller.ResourceQuery[any]) (int, error) {
	return p.store.CountAccounts(ctx, NewListAccountsQuery(ledgercontroller.PaginatedQueryOptions[PITFilterWithVolumes]{
		QueryBuilder: query.Builder,
		Options: PITFilterWithVolumes{
			PITFilter: PITFilter{
				PIT: query.PIT,
				OOT: query.OOT,
			},
			ExpandVolumes:          slices.Contains(query.Expand, "volumes"),
			ExpandEffectiveVolumes: slices.Contains(query.Expand, "effectiveVolumes"),
		},
	}))
}

func (p accountsPaginatedResourceAdapter) Paginate(ctx context.Context, query ledgercontroller.OffsetPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Account], error) {
	return p.store.GetAccountsWithVolumes(ctx, NewListAccountsQuery(ledgercontroller.PaginatedQueryOptions[PITFilterWithVolumes]{
		QueryBuilder: query.Options.Builder,
		PageSize:     query.PageSize,
		Options: PITFilterWithVolumes{
			PITFilter: PITFilter{
				PIT: query.Options.PIT,
				OOT: query.Options.OOT,
			},
			ExpandVolumes:          slices.Contains(query.Options.Expand, "volumes"),
			ExpandEffectiveVolumes: slices.Contains(query.Options.Expand, "effectiveVolumes"),
		},
	}))
}

var _ ledgercontroller.PaginatedResource[ledger.Account, any, ledgercontroller.OffsetPaginatedQuery[any]] = (*accountsPaginatedResourceAdapter)(nil)

type logsPaginatedResourceAdapter struct {
	store *Store
}

func (p logsPaginatedResourceAdapter) GetOne(_ context.Context, _ ledgercontroller.ResourceQuery[any]) (*ledger.Log, error) {
	panic("never used")
}

func (p logsPaginatedResourceAdapter) Count(_ context.Context, _ ledgercontroller.ResourceQuery[any]) (int, error) {
	panic("never used")
}

func (p logsPaginatedResourceAdapter) Paginate(ctx context.Context, query ledgercontroller.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
	return p.store.GetLogs(ctx, NewListLogsQuery(ledgercontroller.PaginatedQueryOptions[any]{
		QueryBuilder: query.Options.Builder,
		PageSize:     query.PageSize,
		Options: PITFilterWithVolumes{
			PITFilter: PITFilter{
				PIT: query.Options.PIT,
				OOT: query.Options.OOT,
			},
		},
	}))
}

var _ ledgercontroller.PaginatedResource[ledger.Log, any, ledgercontroller.ColumnPaginatedQuery[any]] = (*logsPaginatedResourceAdapter)(nil)

type transactionsPaginatedResourceAdapter struct {
	store *Store
}

func (p transactionsPaginatedResourceAdapter) GetOne(ctx context.Context, query ledgercontroller.ResourceQuery[any]) (*ledger.Transaction, error) {
	var id int
	_ = query.Builder.Walk(func(_ string, _ string, value any) error {
		id = value.(int)
		return nil
	})
	return p.store.GetTransactionWithVolumes(ctx, GetTransactionQuery{
		PITFilterWithVolumes: PITFilterWithVolumes{
			PITFilter: PITFilter{
				PIT: query.PIT,
				OOT: query.OOT,
			},
			ExpandVolumes:          slices.Contains(query.Expand, "volumes"),
			ExpandEffectiveVolumes: slices.Contains(query.Expand, "effectiveVolumes"),
		},
		ID: id,
	})
}

func (p transactionsPaginatedResourceAdapter) Count(ctx context.Context, query ledgercontroller.ResourceQuery[any]) (int, error) {
	return p.store.CountTransactions(ctx, NewListTransactionsQuery(ledgercontroller.PaginatedQueryOptions[PITFilterWithVolumes]{
		QueryBuilder: query.Builder,
		Options: PITFilterWithVolumes{
			PITFilter: PITFilter{
				PIT: query.PIT,
				OOT: query.OOT,
			},
			ExpandVolumes:          slices.Contains(query.Expand, "volumes"),
			ExpandEffectiveVolumes: slices.Contains(query.Expand, "effectiveVolumes"),
		},
	}))
}

func (p transactionsPaginatedResourceAdapter) Paginate(ctx context.Context, query ledgercontroller.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Transaction], error) {
	return p.store.GetTransactions(ctx, NewListTransactionsQuery(ledgercontroller.PaginatedQueryOptions[PITFilterWithVolumes]{
		QueryBuilder: query.Options.Builder,
		PageSize:     query.PageSize,
		Options: PITFilterWithVolumes{
			PITFilter: PITFilter{
				PIT: query.Options.PIT,
				OOT: query.Options.OOT,
			},
			ExpandVolumes:          slices.Contains(query.Options.Expand, "volumes"),
			ExpandEffectiveVolumes: slices.Contains(query.Options.Expand, "effectiveVolumes"),
		},
	}))
}

var _ ledgercontroller.PaginatedResource[ledger.Transaction, any, ledgercontroller.ColumnPaginatedQuery[any]] = (*transactionsPaginatedResourceAdapter)(nil)

type aggregatedBalancesPaginatedResourceAdapter struct {
	store *Store
}

func (p aggregatedBalancesPaginatedResourceAdapter) GetOne(ctx context.Context, query ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]) (*ledger.AggregatedVolumes, error) {
	rawRet, err := p.store.GetAggregatedBalances(ctx, GetAggregatedBalanceQuery{
		PITFilter: PITFilter{
			PIT: query.PIT,
			OOT: query.OOT,
		},
		QueryBuilder:     query.Builder,
		UseInsertionDate: query.Opts.UseInsertionDate,
	})
	if err != nil {
		return nil, err
	}

	ret := ledger.AggregatedVolumes{
		Aggregated: ledger.VolumesByAssets{},
	}
	for asset, balance := range rawRet {
		ret.Aggregated[asset] = ledger.Volumes{
			Input:  new(big.Int).Add(new(big.Int), balance),
			Output: new(big.Int),
		}
	}

	return &ret, nil
}

func (p aggregatedBalancesPaginatedResourceAdapter) Count(_ context.Context, _ ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]) (int, error) {
	panic("never used")
}

var _ ledgercontroller.Resource[ledger.AggregatedVolumes, ledgercontroller.GetAggregatedVolumesOptions] = (*aggregatedBalancesPaginatedResourceAdapter)(nil)

type aggregatedVolumesPaginatedResourceAdapter struct {
	store *Store
}

func (p aggregatedVolumesPaginatedResourceAdapter) Paginate(ctx context.Context, query ledgercontroller.OffsetPaginatedQuery[ledgercontroller.GetVolumesOptions]) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	return p.store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(ledgercontroller.PaginatedQueryOptions[FiltersForVolumes]{
		QueryBuilder: query.Options.Builder,
		PageSize:     query.PageSize,
		Options: FiltersForVolumes{
			PITFilter: PITFilter{
				PIT: query.Options.PIT,
				OOT: query.Options.OOT,
			},
			UseInsertionDate: query.Options.Opts.UseInsertionDate,
			GroupLvl:         query.Options.Opts.GroupLvl,
		},
	}))
}

func (p aggregatedVolumesPaginatedResourceAdapter) GetOne(_ context.Context, _ ledgercontroller.ResourceQuery[ledgercontroller.GetVolumesOptions]) (*ledger.VolumesWithBalanceByAssetByAccount, error) {
	panic("never used")
}

func (p aggregatedVolumesPaginatedResourceAdapter) Count(_ context.Context, _ ledgercontroller.ResourceQuery[ledgercontroller.GetVolumesOptions]) (int, error) {
	panic("never used")
}

var _ ledgercontroller.PaginatedResource[ledger.VolumesWithBalanceByAssetByAccount, ledgercontroller.GetVolumesOptions, ledgercontroller.OffsetPaginatedQuery[ledgercontroller.GetVolumesOptions]] = (*aggregatedVolumesPaginatedResourceAdapter)(nil)

type DefaultStoreAdapter struct {
	newStore       *ledgerstore.Store
	legacyStore    *Store
	isFullUpToDate bool
}

func (d *DefaultStoreAdapter) Accounts() ledgercontroller.PaginatedResource[ledger.Account, any, ledgercontroller.OffsetPaginatedQuery[any]] {
	if !d.isFullUpToDate {
		return &accountsPaginatedResourceAdapter{store: d.legacyStore}
	}
	return d.newStore.Accounts()
}

func (d *DefaultStoreAdapter) Logs() ledgercontroller.PaginatedResource[ledger.Log, any, ledgercontroller.ColumnPaginatedQuery[any]] {
	if !d.isFullUpToDate {
		return &logsPaginatedResourceAdapter{store: d.legacyStore}
	}
	return d.newStore.Logs()
}

func (d *DefaultStoreAdapter) Transactions() ledgercontroller.PaginatedResource[ledger.Transaction, any, ledgercontroller.ColumnPaginatedQuery[any]] {
	if !d.isFullUpToDate {
		return &transactionsPaginatedResourceAdapter{store: d.legacyStore}
	}
	return d.newStore.Transactions()
}

func (d *DefaultStoreAdapter) AggregatedBalances() ledgercontroller.Resource[ledger.AggregatedVolumes, ledgercontroller.GetAggregatedVolumesOptions] {
	if !d.isFullUpToDate {
		return &aggregatedBalancesPaginatedResourceAdapter{store: d.legacyStore}
	}
	return d.newStore.AggregatedVolumes()
}

func (d *DefaultStoreAdapter) Volumes() ledgercontroller.PaginatedResource[ledger.VolumesWithBalanceByAssetByAccount, ledgercontroller.GetVolumesOptions, ledgercontroller.OffsetPaginatedQuery[ledgercontroller.GetVolumesOptions]] {
	if !d.isFullUpToDate {
		return &aggregatedVolumesPaginatedResourceAdapter{store: d.legacyStore}
	}
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

func (d *DefaultStoreAdapter) BeginTX(ctx context.Context, opts *sql.TxOptions) (ledgercontroller.Store, *bun.Tx, error) {
	store, tx, err := d.newStore.BeginTX(ctx, opts)
	if err != nil {
		return nil, nil, err
	}

	legacyStore := d.legacyStore.WithDB(store.GetDB())

	return &DefaultStoreAdapter{
		newStore:    store,
		legacyStore: legacyStore,
	}, tx, nil
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
