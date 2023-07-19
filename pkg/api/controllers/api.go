package controllers

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/ledger/command"
	"github.com/formancehq/ledger/pkg/storage/driver"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

//go:generate mockgen -source api.go -destination api_test.go -package controllers_test . Ledger

type Ledger interface {
	GetAccount(ctx context.Context, param string) (*core.AccountWithVolumes, error)
	GetAccounts(ctx context.Context, query ledgerstore.AccountsQuery) (*api.Cursor[core.Account], error)
	CountAccounts(ctx context.Context, query ledgerstore.AccountsQuery) (uint64, error)
	GetBalancesAggregated(ctx context.Context, q ledgerstore.BalancesQuery) (core.BalancesByAssets, error)
	GetBalances(ctx context.Context, q ledgerstore.BalancesQuery) (*api.Cursor[core.BalancesByAssetsByAccounts], error)
	GetMigrationsInfo(ctx context.Context) ([]core.MigrationInfo, error)
	Stats(ctx context.Context) (ledger.Stats, error)
	GetLogs(ctx context.Context, query ledgerstore.LogsQuery) (*api.Cursor[core.ChainedLog], error)
	CountTransactions(ctx context.Context, query ledgerstore.TransactionsQuery) (uint64, error)
	GetTransactions(ctx context.Context, query ledgerstore.TransactionsQuery) (*api.Cursor[core.ExpandedTransaction], error)
	GetTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error)

	CreateTransaction(ctx context.Context, parameters command.Parameters, data core.RunScript) (*core.Transaction, error)
	RevertTransaction(ctx context.Context, parameters command.Parameters, id uint64) (*core.Transaction, error)
	SaveMeta(ctx context.Context, parameters command.Parameters, targetType string, targetID any, m metadata.Metadata) error
}

type Backend interface {
	GetLedger(ctx context.Context, name string) (Ledger, error)
	ListLedgers(ctx context.Context) ([]string, error)
	CloseLedgers(ctx context.Context) error
	GetVersion() string
}

type DefaultBackend struct {
	storageDriver *driver.Driver
	resolver      *ledger.Resolver
	version       string
}

func (d DefaultBackend) GetLedger(ctx context.Context, name string) (Ledger, error) {
	return d.resolver.GetLedger(ctx, name)
}

func (d DefaultBackend) ListLedgers(ctx context.Context) ([]string, error) {
	return d.storageDriver.GetSystemStore().ListLedgers(ctx)
}

func (d DefaultBackend) CloseLedgers(ctx context.Context) error {
	return d.resolver.CloseLedgers(ctx)
}

func (d DefaultBackend) GetVersion() string {
	return d.version
}

var _ Backend = (*DefaultBackend)(nil)

func NewDefaultBackend(driver *driver.Driver, version string, resolver *ledger.Resolver) *DefaultBackend {
	return &DefaultBackend{
		storageDriver: driver,
		resolver:      resolver,
		version:       version,
	}
}
