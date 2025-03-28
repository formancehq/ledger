package ledger

import (
	"context"
	"database/sql"
	"github.com/formancehq/ledger/internal/storage/resources"
	"math/big"

	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/numscript"

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

	Accounts() resources.PaginatedResource[ledger.Account, any, resources.OffsetPaginatedQuery[any]]
	Logs() resources.PaginatedResource[ledger.Log, any, resources.ColumnPaginatedQuery[any]]
	Transactions() resources.PaginatedResource[ledger.Transaction, any, resources.ColumnPaginatedQuery[any]]
	AggregatedBalances() resources.Resource[ledger.AggregatedVolumes, GetAggregatedVolumesOptions]
	Volumes() resources.PaginatedResource[ledger.VolumesWithBalanceByAssetByAccount, GetVolumesOptions, resources.OffsetPaginatedQuery[GetVolumesOptions]]
}

type vmStoreAdapter struct {
	Store
}

func (v *vmStoreAdapter) GetAccount(ctx context.Context, address string) (*ledger.Account, error) {
	account, err := v.Store.Accounts().GetOne(ctx, resources.ResourceQuery[any]{
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

func NewListLedgersQuery(pageSize uint64) resources.ColumnPaginatedQuery[any] {
	return resources.ColumnPaginatedQuery[any]{
		PageSize: pageSize,
		Column:   "id",
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
		v, err := s.Store.Accounts().GetOne(ctx, resources.ResourceQuery[any]{
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
