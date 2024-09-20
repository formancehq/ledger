package backend

import (
	"context"
	"math/big"

	"github.com/formancehq/go-libs/bun/bunpaginate"

	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/migrations"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/engine"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/systemstore"
)

//go:generate mockgen -source backend.go -destination backend_generated.go -package backend . Ledger

type Ledger interface {
	GetAccountWithVolumes(ctx context.Context, query ledgerstore.GetAccountQuery) (*ledger.ExpandedAccount, error)
	GetAccountsWithVolumes(ctx context.Context, query ledgerstore.GetAccountsQuery) (*bunpaginate.Cursor[ledger.ExpandedAccount], error)
	CountAccounts(ctx context.Context, query ledgerstore.GetAccountsQuery) (int, error)
	GetAggregatedBalances(ctx context.Context, q ledgerstore.GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error)
	GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error)
	Stats(ctx context.Context) (engine.Stats, error)
	GetLogs(ctx context.Context, query ledgerstore.GetLogsQuery) (*bunpaginate.Cursor[ledger.ChainedLog], error)
	CountTransactions(ctx context.Context, query ledgerstore.GetTransactionsQuery) (int, error)
	GetTransactions(ctx context.Context, query ledgerstore.GetTransactionsQuery) (*bunpaginate.Cursor[ledger.ExpandedTransaction], error)
	GetTransactionWithVolumes(ctx context.Context, query ledgerstore.GetTransactionQuery) (*ledger.ExpandedTransaction, error)

	CreateTransaction(ctx context.Context, parameters command.Parameters, data ledger.RunScript) (*ledger.Transaction, error)
	RevertTransaction(ctx context.Context, parameters command.Parameters, id *big.Int, force, atEffectiveDate bool) (*ledger.Transaction, error)
	SaveMeta(ctx context.Context, parameters command.Parameters, targetType string, targetID any, m metadata.Metadata) error
	DeleteMetadata(ctx context.Context, parameters command.Parameters, targetType string, targetID any, key string) error
	Import(ctx context.Context, stream chan *ledger.ChainedLog) error
	Export(ctx context.Context, w engine.ExportWriter) error

	IsDatabaseUpToDate(ctx context.Context) (bool, error)

	GetVolumesWithBalances(ctx context.Context, q ledgerstore.GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error)
}

type Backend interface {
	GetLedgerEngine(ctx context.Context, name string) (Ledger, error)
	GetLedger(ctx context.Context, name string) (*systemstore.Ledger, error)
	ListLedgers(ctx context.Context, query systemstore.ListLedgersQuery) (*bunpaginate.Cursor[systemstore.Ledger], error)
	CreateLedger(ctx context.Context, name string, configuration driver.LedgerConfiguration) error
	UpdateLedgerMetadata(ctx context.Context, name string, m map[string]string) error
	GetVersion() string
	DeleteLedgerMetadata(ctx context.Context, param string, key string) error
}

type DefaultBackend struct {
	storageDriver *driver.Driver
	resolver      *engine.Resolver
	version       string
}

func (d DefaultBackend) DeleteLedgerMetadata(ctx context.Context, name string, key string) error {
	return d.storageDriver.GetSystemStore().DeleteLedgerMetadata(ctx, name, key)
}

func (d DefaultBackend) UpdateLedgerMetadata(ctx context.Context, name string, m map[string]string) error {
	return d.storageDriver.GetSystemStore().UpdateLedgerMetadata(ctx, name, m)
}

func (d DefaultBackend) GetLedger(ctx context.Context, name string) (*systemstore.Ledger, error) {
	return d.storageDriver.GetSystemStore().GetLedger(ctx, name)
}

func (d DefaultBackend) CreateLedger(ctx context.Context, name string, configuration driver.LedgerConfiguration) error {
	_, err := d.resolver.CreateLedger(ctx, name, configuration)

	return err
}

func (d DefaultBackend) GetLedgerEngine(ctx context.Context, name string) (Ledger, error) {
	return d.resolver.GetLedger(ctx, name)
}

func (d DefaultBackend) ListLedgers(ctx context.Context, query systemstore.ListLedgersQuery) (*bunpaginate.Cursor[systemstore.Ledger], error) {
	return d.storageDriver.GetSystemStore().ListLedgers(ctx, query)
}

func (d DefaultBackend) GetVersion() string {
	return d.version
}

var _ Backend = (*DefaultBackend)(nil)

func NewDefaultBackend(driver *driver.Driver, version string, resolver *engine.Resolver) *DefaultBackend {
	return &DefaultBackend{
		storageDriver: driver,
		resolver:      resolver,
		version:       version,
	}
}
