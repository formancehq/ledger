package ledger

import (
	"context"
	"database/sql"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/ledger/internal/storage/common"
	"math/big"

	"github.com/formancehq/go-libs/v3/migrations"
	"github.com/formancehq/numscript"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/go-libs/v3/time"
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
	BeginTX(ctx context.Context, options *sql.TxOptions) (Store, *bun.Tx, error)
	Commit() error
	Rollback() error

	// GetBalances must returns balance and lock account until the end of the TX
	GetBalances(ctx context.Context, query BalanceQuery) (Balances, error)
	CommitTransaction(ctx context.Context, transaction *ledger.Transaction, accountMetadata map[string]metadata.Metadata) error
	// RevertTransaction revert the transaction with identifier id
	// It returns :
	//  * the reverted transaction
	//  * a boolean indicating if the transaction has been reverted. false indicates an already reverted transaction (unless error != nil)
	//  * an error
	RevertTransaction(ctx context.Context, id int, at time.Time) (*ledger.Transaction, bool, error)
	UpdateTransactionMetadata(ctx context.Context, transactionID int, m metadata.Metadata, at time.Time) (*ledger.Transaction, bool, error)
	DeleteTransactionMetadata(ctx context.Context, transactionID int, key string, at time.Time) (*ledger.Transaction, bool, error)
	UpdateAccountsMetadata(ctx context.Context, m map[string]metadata.Metadata) error
	// UpsertAccount returns a boolean indicating if the account was upserted
	UpsertAccounts(ctx context.Context, accounts ...*ledger.Account) error
	DeleteAccountMetadata(ctx context.Context, address, key string) error
	InsertLog(ctx context.Context, log *ledger.Log) error

	LockLedger(ctx context.Context) (Store, bun.IDB, func() error, error)

	GetDB() bun.IDB
	ReadLogWithIdempotencyKey(ctx context.Context, ik string) (*ledger.Log, error)

	IsUpToDate(ctx context.Context) (bool, error)
	GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error)

	Accounts() common.PaginatedResource[ledger.Account, any, common.OffsetPaginatedQuery[any]]
	Logs() common.PaginatedResource[ledger.Log, any, common.ColumnPaginatedQuery[any]]
	Transactions() common.PaginatedResource[ledger.Transaction, any, common.ColumnPaginatedQuery[any]]
	AggregatedBalances() common.Resource[ledger.AggregatedVolumes, GetAggregatedVolumesOptions]
	Volumes() common.PaginatedResource[ledger.VolumesWithBalanceByAssetByAccount, GetVolumesOptions, common.OffsetPaginatedQuery[GetVolumesOptions]]
}

type vmStoreAdapter struct {
	Store
}

func (v *vmStoreAdapter) GetAccount(ctx context.Context, address string) (*ledger.Account, error) {
	account, err := v.Store.Accounts().GetOne(ctx, common.ResourceQuery[any]{
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

func NewListLedgersQuery(pageSize uint64) common.ColumnPaginatedQuery[any] {
	return common.ColumnPaginatedQuery[any]{
		PageSize: pageSize,
		Column:   "id",
		Order:    (*bunpaginate.Order)(pointer.For(bunpaginate.OrderAsc)),
		Options: common.ResourceQuery[any]{
			Expand: make([]string, 0),
		},
	}
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
		v, err := s.Store.Accounts().GetOne(ctx, common.ResourceQuery[any]{
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
