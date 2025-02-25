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

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package ledger . Store
type Store interface {
	BeginTX(ctx context.Context, options *sql.TxOptions) (Store, error)
	Commit() error
	Rollback() error

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
	UpsertAccounts(ctx context.Context, accounts ...*ledger.Account) error
	DeleteAccountMetadata(ctx context.Context, address, key string) error
	InsertLog(ctx context.Context, log *ledger.Log) error

	LockLedger(ctx context.Context) error

	GetDB() bun.IDB
	ReadLogWithIdempotencyKey(ctx context.Context, ik string) (*ledger.Log, error)

	IsUpToDate(ctx context.Context) (bool, error)
	GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error)

	Accounts() PaginatedResource[ledger.Account, any, OffsetPaginatedQuery[any]]
	Logs() PaginatedResource[ledger.Log, any, ColumnPaginatedQuery[any]]
	Transactions() PaginatedResource[ledger.Transaction, any, ColumnPaginatedQuery[any]]
	AggregatedBalances() Resource[ledger.AggregatedVolumes, GetAggregatedVolumesOptions]
	Volumes() PaginatedResource[ledger.VolumesWithBalanceByAssetByAccount, GetVolumesOptions, OffsetPaginatedQuery[GetVolumesOptions]]
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

type vmStoreAdapter struct {
	Store
}

func (v *vmStoreAdapter) GetAccount(ctx context.Context, address string) (*ledger.Account, error) {
	account, err := v.Store.Accounts().GetOne(ctx, ResourceQuery[any]{
		Builder: query.Match("address", address),
	})
	if err != nil {
		return nil, err
	}
	return account, nil
}

var _ vm.Store = (*vmStoreAdapter)(nil)

func newVmStoreAdapter(tx Store) *vmStoreAdapter {
	return &vmStoreAdapter{
		Store: tx,
	}
}

type ListLedgersQueryPayload struct {
	Bucket string
}

type ListLedgersQuery bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[ListLedgersQueryPayload]]

func (q ListLedgersQuery) WithBucket(bucket string) ListLedgersQuery {
	q.Options.Options.Bucket = bucket

	return q
}

func NewListLedgersQuery(pageSize uint64) ListLedgersQuery {
	return ListLedgersQuery{
		PageSize: pageSize,
	}
}

type ResourceQuery[Opts any] struct {
	PIT     *time.Time    `json:"pit"`
	OOT     *time.Time    `json:"oot"`
	Builder query.Builder `json:"qb"`
	Expand  []string      `json:"expand,omitempty"`
	Opts    Opts          `json:"opts"`
}

func (rq ResourceQuery[Opts]) UsePIT() bool {
	return rq.PIT != nil && !rq.PIT.IsZero()
}

func (rq ResourceQuery[Opts]) UseOOT() bool {
	return rq.OOT != nil && !rq.OOT.IsZero()
}

func (rq *ResourceQuery[Opts]) UnmarshalJSON(data []byte) error {
	type rawResourceQuery ResourceQuery[Opts]
	type aux struct {
		rawResourceQuery
		Builder json.RawMessage `json:"qb"`
	}
	x := aux{}
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}

	var err error
	*rq = ResourceQuery[Opts](x.rawResourceQuery)
	rq.Builder, err = query.ParseJSON(string(x.Builder))

	return err
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package ledger . Resource
type Resource[ResourceType, OptionsType any] interface {
	GetOne(ctx context.Context, query ResourceQuery[OptionsType]) (*ResourceType, error)
	Count(ctx context.Context, query ResourceQuery[OptionsType]) (int, error)
}

type (
	OffsetPaginatedQuery[OptionsType any] struct {
		Column   string                     `json:"column"`
		Offset   uint64                     `json:"offset"`
		Order    *bunpaginate.Order         `json:"order"`
		PageSize uint64                     `json:"pageSize"`
		Options  ResourceQuery[OptionsType] `json:"filters"`
	}
	ColumnPaginatedQuery[OptionsType any] struct {
		PageSize     uint64   `json:"pageSize"`
		Bottom       *big.Int `json:"bottom"`
		Column       string   `json:"column"`
		PaginationID *big.Int `json:"paginationID"`
		// todo: backport in go-libs
		Order   *bunpaginate.Order         `json:"order"`
		Options ResourceQuery[OptionsType] `json:"filters"`
		Reverse bool                       `json:"reverse"`
	}
	PaginatedQuery[OptionsType any] interface {
		OffsetPaginatedQuery[OptionsType] | ColumnPaginatedQuery[OptionsType]
	}
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package ledger . PaginatedResource
type PaginatedResource[ResourceType, OptionsType any, PaginationQueryType PaginatedQuery[OptionsType]] interface {
	Resource[ResourceType, OptionsType]
	Paginate(ctx context.Context, paginationOptions PaginationQueryType) (*bunpaginate.Cursor[ResourceType], error)
}

// numscript rewrite implementation

var _ numscript.Store = (*numscriptRewriteAdapter)(nil)

func newNumscriptRewriteAdapter(store Store) *numscriptRewriteAdapter {
	return &numscriptRewriteAdapter{
		Store: store,
	}
}

type numscriptRewriteAdapter struct {
	Store Store
}

func (s *numscriptRewriteAdapter) GetBalances(ctx context.Context, q numscript.BalanceQuery) (numscript.Balances, error) {
	vmBalances, err := s.Store.GetBalances(ctx, BalanceQuery(q))
	if err != nil {
		return nil, err
	}

	return numscript.Balances(vmBalances), nil
}

func (s *numscriptRewriteAdapter) GetAccountsMetadata(ctx context.Context, q numscript.MetadataQuery) (numscript.AccountsMetadata, error) {
	m := numscript.AccountsMetadata{}

	// we ignore the needed metadata values and just return all of them
	for address := range q {
		v, err := s.Store.Accounts().GetOne(ctx, ResourceQuery[any]{
			Builder: query.Match("address", address),
		})
		if err != nil {
			return nil, err
		}
		m[v.Address] = v.Metadata
	}

	return m, nil
}

type GetAggregatedVolumesOptions struct {
	UseInsertionDate bool `json:"useInsertionDate"`
}

type GetVolumesOptions struct {
	UseInsertionDate bool `json:"useInsertionDate"`
	GroupLvl         int  `json:"groupLvl"`
}
