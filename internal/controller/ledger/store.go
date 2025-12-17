package ledger

import (
	"context"
	"database/sql"
	"math/big"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/migrations"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/numscript"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine/vm"
	"github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

type Balance struct {
	Asset   string
	Balance *big.Int
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package ledger . Store
type Store interface {
	BeginTX(ctx context.Context, options *sql.TxOptions) (Store, *bun.Tx, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error

	// GetBalances must returns balance and lock account until the end of the TX
	GetBalances(ctx context.Context, query ledgerstore.BalanceQuery) (ledger.Balances, error)
	CommitTransaction(ctx context.Context, transaction *ledger.Transaction) error
	// RevertTransaction revert the transaction with identifier id
	// It returns :
	//  * the reverted transaction
	//  * a boolean indicating if the transaction has been reverted. false indicates an already reverted transaction (unless error != nil)
	//  * an error
	RevertTransaction(ctx context.Context, id uint64, at time.Time) (*ledger.Transaction, bool, error)
	UpdateTransactionMetadata(ctx context.Context, transactionID uint64, m metadata.Metadata, at time.Time) (*ledger.Transaction, bool, error)
	DeleteTransactionMetadata(ctx context.Context, transactionID uint64, key string, at time.Time) (*ledger.Transaction, bool, error)
	UpdateAccountsMetadata(ctx context.Context, m map[string]metadata.Metadata, at time.Time) error
	// UpsertAccount returns a boolean indicating if the account was upserted
	UpsertAccounts(ctx context.Context, accounts ...ledger.AccountWithDefaultMetadata) error
	DeleteAccountMetadata(ctx context.Context, address, key string) error
	InsertSchema(ctx context.Context, data *ledger.Schema) error
	FindSchema(ctx context.Context, version string) (*ledger.Schema, error)
	FindSchemas(ctx context.Context, query common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Schema], error)
	FindLatestSchemaVersion(ctx context.Context) (*string, error)
	InsertLog(ctx context.Context, log *ledger.Log) error

	LockLedger(ctx context.Context) (Store, bun.IDB, func() error, error)

	ReadLogWithIdempotencyKey(ctx context.Context, ik string) (*ledger.Log, error)

	IsUpToDate(ctx context.Context) (bool, error)
	GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error)

	Accounts() common.PaginatedResource[ledger.Account, any]
	Logs() common.PaginatedResource[ledger.Log, any]
	Transactions() common.PaginatedResource[ledger.Transaction, any]
	AggregatedBalances() common.Resource[ledger.AggregatedVolumes, ledgerstore.GetAggregatedVolumesOptions]
	Volumes() common.PaginatedResource[ledger.VolumesWithBalanceByAssetByAccount, ledgerstore.GetVolumesOptions]
}

type vmStoreAdapter struct {
	Store
}

func (v *vmStoreAdapter) GetBalances(ctx context.Context, query vm.BalanceQuery) (vm.Balances, error) {
	return v.Store.GetBalances(ctx, query)
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
	vmBalances, err := s.Store.GetBalances(ctx, q)
	if err != nil {
		return nil, err
	}

	return vmBalances, nil
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
