package analytics

import (
	"context"

	storageerrors "github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

//go:generate mockgen -source backend.go -destination backend_test.go -package analytics . Ledger

type Ledger interface {
	CountTransactions(ctx context.Context) (int, error)
	CountAccounts(ctx context.Context) (int, error)
}

type defaultLedger struct {
	store *ledgerstore.Store
}

func (d defaultLedger) CountTransactions(ctx context.Context) (int, error) {
	return d.store.CountTransactions(ctx, ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{})))
}

func (d defaultLedger) CountAccounts(ctx context.Context) (int, error) {
	return d.store.CountAccounts(ctx, ledgerstore.NewGetAccountsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{})))
}

var _ Ledger = (*defaultLedger)(nil)

type Backend interface {
	AppID(ctx context.Context) (string, error)
	ListLedgers(ctx context.Context) ([]string, error)
	GetLedgerStore(ctx context.Context, l string) (Ledger, error)
}

type defaultBackend struct {
	driver *driver.Driver
	appID  string
}

func (d defaultBackend) AppID(ctx context.Context) (string, error) {
	var err error
	if d.appID == "" {
		d.appID, err = d.driver.GetSystemStore().GetConfiguration(ctx, "appId")
		if err != nil && !errors.Is(err, storageerrors.ErrNotFound) {
			return "", err
		}
		if errors.Is(err, storageerrors.ErrNotFound) {
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

func (d defaultBackend) GetLedgerStore(ctx context.Context, name string) (Ledger, error) {
	ledgerStore, err := d.driver.GetLedgerStore(ctx, name)
	if err != nil {
		return nil, err
	}
	return &defaultLedger{
		store: ledgerStore,
	}, nil
}

var _ Backend = (*defaultBackend)(nil)

func newDefaultBackend(driver *driver.Driver, appID string) *defaultBackend {
	return &defaultBackend{
		driver: driver,
		appID:  appID,
	}
}
