package analytics

import (
	"context"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

//go:generate mockgen -source backend.go -destination backend_test.go -package analytics . Ledger

type Ledger interface {
	CountTransactions(ctx context.Context) (uint64, error)
	CountAccounts(ctx context.Context) (uint64, error)
}

type defaultLedger struct {
	store storage.LedgerStore
}

func (d defaultLedger) CountTransactions(ctx context.Context) (uint64, error) {
	return d.store.CountTransactions(ctx, *storage.NewTransactionsQuery())
}

func (d defaultLedger) CountAccounts(ctx context.Context) (uint64, error) {
	return d.store.CountAccounts(ctx, *storage.NewAccountsQuery())
}

var _ Ledger = (*defaultLedger)(nil)

type Backend interface {
	AppID(ctx context.Context) (string, error)
	ListLedgers(ctx context.Context) ([]string, error)
	GetLedgerStore(ctx context.Context, l string, b bool) (Ledger, bool, error)
}

type defaultBackend struct {
	driver storage.Driver
	appID  string
}

func (d defaultBackend) AppID(ctx context.Context) (string, error) {
	var err error
	if d.appID == "" {
		d.appID, err = d.driver.GetSystemStore().GetConfiguration(ctx, "appId")
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return "", err
		}
		if errors.Is(err, storage.ErrNotFound) {
			d.appID = uuid.NewString()
			if err := d.driver.GetSystemStore().InsertConfiguration(ctx, "appId", d.appID); err != nil {
				return "", err
			}
		}
	}
	return d.appID, nil
}

func (d defaultBackend) ListLedgers(ctx context.Context) ([]string, error) {
	return d.driver.GetSystemStore().ListLedgers(ctx)
}

func (d defaultBackend) GetLedgerStore(ctx context.Context, name string, create bool) (Ledger, bool, error) {
	ledgerStore, created, err := d.driver.GetLedgerStore(ctx, name, create)
	if err != nil {
		return nil, false, err
	}
	return &defaultLedger{
		store: ledgerStore,
	}, created, nil
}

var _ Backend = (*defaultBackend)(nil)

func newDefaultBackend(driver storage.Driver, appID string) *defaultBackend {
	return &defaultBackend{
		driver: driver,
		appID:  appID,
	}
}
