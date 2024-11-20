package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"math/big"

	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/numscript"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine/vm"
	"github.com/uptrace/bun"
)

type Balance struct {
	Asset   string
	Balance *big.Int
}

type BalanceQuery = vm.BalanceQuery
type Balances = vm.Balances

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package ledger . TX
type TX interface {
	GetAccount(ctx context.Context, query GetAccountQuery) (*ledger.Account, error)
	// GetBalances must returns balance and lock account until the end of the TX
	GetBalances(ctx context.Context, query BalanceQuery) (Balances, error)
	CommitTransaction(ctx context.Context, transaction *ledger.Transaction) error
	// RevertTransaction revert the transaction with identifier id
	// It returns :
	//  * the reverted transaction
	//  * a boolean indicating if the transaction has been reverted. false indicates an already reverted transaction (unless error != nil)
	//  * an error
	RevertTransaction(ctx context.Context, id int, at time.Time) (*ledger.Transaction, bool, error)
	UpdateTransactionMetadata(ctx context.Context, transactionID int, m metadata.Metadata) (*ledger.Transaction, bool, error)
	DeleteTransactionMetadata(ctx context.Context, transactionID int, key string) (*ledger.Transaction, bool, error)
	UpdateAccountsMetadata(ctx context.Context, m map[string]metadata.Metadata) error
	// UpsertAccount returns a boolean indicating if the account was upserted
	UpsertAccount(ctx context.Context, account *ledger.Account) (bool, error)
	DeleteAccountMetadata(ctx context.Context, address, key string) error
	InsertLog(ctx context.Context, log *ledger.Log) error

	LockLedger(ctx context.Context) error
	ListLogs(ctx context.Context, q GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error)
}

type Store interface {
	WithTX(context.Context, *sql.TxOptions, func(TX) (bool, error)) error
	GetDB() bun.IDB
	ListLogs(ctx context.Context, q GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error)
	ReadLogWithIdempotencyKey(ctx context.Context, ik string) (*ledger.Log, error)

	ListTransactions(ctx context.Context, q ListTransactionsQuery) (*bunpaginate.Cursor[ledger.Transaction], error)
	CountTransactions(ctx context.Context, q ListTransactionsQuery) (int, error)
	GetTransaction(ctx context.Context, query GetTransactionQuery) (*ledger.Transaction, error)
	CountAccounts(ctx context.Context, a ListAccountsQuery) (int, error)
	ListAccounts(ctx context.Context, a ListAccountsQuery) (*bunpaginate.Cursor[ledger.Account], error)
	GetAccount(ctx context.Context, q GetAccountQuery) (*ledger.Account, error)
	GetAggregatedBalances(ctx context.Context, q GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error)
	GetVolumesWithBalances(ctx context.Context, q GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error)
	IsUpToDate(ctx context.Context) (bool, error)
	GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error)
}

type ListTransactionsQuery bunpaginate.ColumnPaginatedQuery[PaginatedQueryOptions[PITFilterWithVolumes]]

func (q ListTransactionsQuery) WithColumn(column string) ListTransactionsQuery {
	ret := pointer.For((bunpaginate.ColumnPaginatedQuery[PaginatedQueryOptions[PITFilterWithVolumes]])(q))
	ret = ret.WithColumn(column)

	return ListTransactionsQuery(*ret)
}

func NewListTransactionsQuery(options PaginatedQueryOptions[PITFilterWithVolumes]) ListTransactionsQuery {
	return ListTransactionsQuery{
		PageSize: options.PageSize,
		Column:   "id",
		Order:    bunpaginate.OrderDesc,
		Options:  options,
	}
}

type GetTransactionQuery struct {
	PITFilterWithVolumes
	ID int
}

func (q GetTransactionQuery) WithExpandVolumes() GetTransactionQuery {
	q.ExpandVolumes = true

	return q
}

func (q GetTransactionQuery) WithExpandEffectiveVolumes() GetTransactionQuery {
	q.ExpandEffectiveVolumes = true

	return q
}

func NewGetTransactionQuery(id int) GetTransactionQuery {
	return GetTransactionQuery{
		PITFilterWithVolumes: PITFilterWithVolumes{},
		ID:                   id,
	}
}

type ListAccountsQuery bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[PITFilterWithVolumes]]

func (q ListAccountsQuery) WithExpandVolumes() ListAccountsQuery {
	q.Options.Options.ExpandVolumes = true

	return q
}

func (q ListAccountsQuery) WithExpandEffectiveVolumes() ListAccountsQuery {
	q.Options.Options.ExpandEffectiveVolumes = true

	return q
}

func NewListAccountsQuery(opts PaginatedQueryOptions[PITFilterWithVolumes]) ListAccountsQuery {
	return ListAccountsQuery{
		PageSize: opts.PageSize,
		Order:    bunpaginate.OrderAsc,
		Options:  opts,
	}
}

type GetAccountQuery struct {
	PITFilterWithVolumes
	Addr string
}

func (q GetAccountQuery) WithPIT(pit time.Time) GetAccountQuery {
	q.PIT = &pit

	return q
}

func (q GetAccountQuery) WithExpandVolumes() GetAccountQuery {
	q.ExpandVolumes = true

	return q
}

func (q GetAccountQuery) WithExpandEffectiveVolumes() GetAccountQuery {
	q.ExpandEffectiveVolumes = true

	return q
}

func NewGetAccountQuery(addr string) GetAccountQuery {
	return GetAccountQuery{
		Addr: addr,
	}
}

type GetAggregatedBalanceQuery struct {
	PITFilter
	QueryBuilder     query.Builder
	UseInsertionDate bool
}

func NewGetAggregatedBalancesQuery(filter PITFilter, qb query.Builder, useInsertionDate bool) GetAggregatedBalanceQuery {
	return GetAggregatedBalanceQuery{
		PITFilter:        filter,
		QueryBuilder:     qb,
		UseInsertionDate: useInsertionDate,
	}
}

type GetVolumesWithBalancesQuery bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[FiltersForVolumes]]

func NewGetVolumesWithBalancesQuery(opts PaginatedQueryOptions[FiltersForVolumes]) GetVolumesWithBalancesQuery {
	return GetVolumesWithBalancesQuery{
		PageSize: opts.PageSize,
		Order:    bunpaginate.OrderAsc,
		Options:  opts,
	}
}

type PaginatedQueryOptions[T any] struct {
	QueryBuilder query.Builder `json:"qb"`
	PageSize     uint64        `json:"pageSize"`
	Options      T             `json:"options"`
}

func (opts *PaginatedQueryOptions[T]) UnmarshalJSON(data []byte) error {
	type aux struct {
		QueryBuilder json.RawMessage `json:"qb"`
		PageSize     uint64          `json:"pageSize"`
		Options      T               `json:"options"`
	}
	x := &aux{}
	if err := json.Unmarshal(data, x); err != nil {
		return err
	}

	*opts = PaginatedQueryOptions[T]{
		PageSize: x.PageSize,
		Options:  x.Options,
	}

	var err error
	if x.QueryBuilder != nil {
		opts.QueryBuilder, err = query.ParseJSON(string(x.QueryBuilder))
		if err != nil {
			return err
		}
	}

	return nil
}

func (opts PaginatedQueryOptions[T]) WithQueryBuilder(qb query.Builder) PaginatedQueryOptions[T] {
	opts.QueryBuilder = qb

	return opts
}

func (opts PaginatedQueryOptions[T]) WithPageSize(pageSize uint64) PaginatedQueryOptions[T] {
	opts.PageSize = pageSize

	return opts
}

func NewPaginatedQueryOptions[T any](options T) PaginatedQueryOptions[T] {
	return PaginatedQueryOptions[T]{
		Options:  options,
		PageSize: bunpaginate.QueryDefaultPageSize,
	}
}

type PITFilter struct {
	PIT *time.Time `json:"pit"`
	OOT *time.Time `json:"oot"`
}

type PITFilterWithVolumes struct {
	PITFilter
	ExpandVolumes          bool `json:"volumes"`
	ExpandEffectiveVolumes bool `json:"effectiveVolumes"`
}

type FiltersForVolumes struct {
	PITFilter
	UseInsertionDate bool
	GroupLvl         int
}

type GetLogsQuery bunpaginate.ColumnPaginatedQuery[PaginatedQueryOptions[any]]

func (q GetLogsQuery) WithOrder(order bunpaginate.Order) GetLogsQuery {
	q.Order = order
	return q
}

func NewListLogsQuery(options PaginatedQueryOptions[any]) GetLogsQuery {
	return GetLogsQuery{
		PageSize: options.PageSize,
		Column:   "id",
		Order:    bunpaginate.OrderDesc,
		Options:  options,
	}
}

type vmStoreAdapter struct {
	TX
}

func (v *vmStoreAdapter) GetAccount(ctx context.Context, address string) (*ledger.Account, error) {
	account, err := v.TX.GetAccount(ctx, NewGetAccountQuery(address))
	if err != nil {
		return nil, err
	}
	return account, nil
}

var _ vm.Store = (*vmStoreAdapter)(nil)

func newVmStoreAdapter(tx TX) *vmStoreAdapter {
	return &vmStoreAdapter{
		TX: tx,
	}
}

type ListLedgersQuery bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[struct{}]]

func NewListLedgersQuery(pageSize uint64) ListLedgersQuery {
	return ListLedgersQuery{
		PageSize: pageSize,
	}
}

// numscript rewrite implementation

var _ numscript.Store = (*numscriptRewriteAdapter)(nil)

func newNumscriptRewriteAdapter(tx TX) *numscriptRewriteAdapter {
	return &numscriptRewriteAdapter{
		TX: tx,
	}
}

type numscriptRewriteAdapter struct {
	TX TX
}

func (s *numscriptRewriteAdapter) GetBalances(ctx context.Context, q numscript.BalanceQuery) (numscript.Balances, error) {
	vmBalances, err := s.TX.GetBalances(ctx, BalanceQuery(q))
	if err != nil {
		return nil, err
	}
	return numscript.Balances(vmBalances), nil
}

func (s *numscriptRewriteAdapter) GetAccountsMetadata(ctx context.Context, q numscript.MetadataQuery) (numscript.AccountsMetadata, error) {
	m := numscript.AccountsMetadata{}

	// we ignore the needed metadata values and just return all of them
	for address := range q {
		v, err := s.TX.GetAccount(ctx, GetAccountQuery{
			Addr: address,
		})
		if err != nil {
			return nil, err
		}
		m[v.Address] = v.Metadata
	}

	return m, nil
}
