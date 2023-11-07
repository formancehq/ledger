package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/systemstore"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
	"go.nhat.io/otelsql"
)

const SystemSchema = "_system"

type pgxDriver struct {
	driverName string
}

var pgxSqlDriver pgxDriver

type otelSQLDriverWithCheckNamedValueDisabled struct {
	driver.Driver
}

func (d otelSQLDriverWithCheckNamedValueDisabled) CheckNamedValue(*driver.NamedValue) error {
	return nil
}

var _ = driver.NamedValueChecker(&otelSQLDriverWithCheckNamedValueDisabled{})

func init() {
	// Default mapping for app driver/sql driver
	pgxSqlDriver.driverName = "pgx"
}

// todo: since se use pq, this is probably useless
func InstrumentalizeSQLDriver() {
	// otelsql has a function Register which wrap the underlying driver, but does not mirror driver.NamedValuedChecker interface of the underlying driver
	// pgx implements this interface and just return nil
	// so, we need to manually wrap the driver to implements this interface and return a nil error
	db, err := sql.Open("pgx", "")
	if err != nil {
		panic(err)
	}

	dri := db.Driver()

	if err = db.Close(); err != nil {
		panic(err)
	}

	wrappedDriver := otelsql.Wrap(dri,
		otelsql.AllowRoot(),
		otelsql.TraceAll(),
	)

	pgxSqlDriver.driverName = fmt.Sprintf("otel-%s", pgxSqlDriver.driverName)
	sql.Register(pgxSqlDriver.driverName, otelSQLDriverWithCheckNamedValueDisabled{
		wrappedDriver,
	})
}

type Driver struct {
	db          *bun.DB
	systemStore *systemstore.Store
	lock        sync.Mutex
}

func (d *Driver) GetSystemStore() *systemstore.Store {
	return d.systemStore
}

func (d *Driver) newStore(name string) (*ledgerstore.Store, error) {
	return ledgerstore.New(d.db, name, func(ctx context.Context) error {
		return d.GetSystemStore().DeleteLedger(ctx, name)
	})
}

func (d *Driver) createLedgerStore(ctx context.Context, name string) (*ledgerstore.Store, error) {
	if name == SystemSchema {
		return nil, errors.New("reserved name")
	}

	exists, err := d.systemStore.Exists(ctx, name)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, storage.ErrStoreAlreadyExists
	}

	_, err = d.systemStore.Register(ctx, name)
	if err != nil {
		return nil, err
	}

	store, err := d.newStore(name)
	if err != nil {
		return nil, err
	}

	_, err = store.Migrate(ctx)

	return store, err
}

func (d *Driver) CreateLedgerStore(ctx context.Context, name string) (*ledgerstore.Store, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	return d.createLedgerStore(ctx, name)
}

func (d *Driver) GetLedgerStore(ctx context.Context, name string) (*ledgerstore.Store, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	exists, err := d.systemStore.Exists(ctx, name)
	if err != nil {
		return nil, err
	}

	var store *ledgerstore.Store
	if !exists {
		store, err = d.createLedgerStore(ctx, name)
	} else {
		store, err = d.newStore(name)
	}
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (d *Driver) Initialize(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Initialize driver")

	_, err := d.db.ExecContext(ctx, "create extension if not exists pgcrypto")
	if err != nil {
		return storage.PostgresError(err)
	}

	_, err = d.db.ExecContext(ctx, fmt.Sprintf(`create schema if not exists "%s"`, SystemSchema))
	if err != nil {
		return storage.PostgresError(err)
	}

	d.systemStore = systemstore.NewStore(d.db)

	if err := d.systemStore.Initialize(ctx); err != nil {
		return err
	}

	return nil
}

func (d *Driver) UpgradeAllLedgersSchemas(ctx context.Context) error {
	systemStore := d.GetSystemStore()
	ledgers, err := systemStore.ListLedgers(ctx)
	if err != nil {
		return err
	}

	for _, ledger := range ledgers {
		store, err := d.GetLedgerStore(ctx, ledger)
		if err != nil {
			return err
		}

		logging.FromContext(ctx).Infof("Upgrading storage '%s'", ledger)
		if _, err := store.Migrate(ctx); err != nil {
			return err
		}
	}

	return nil
}

func New(db *bun.DB) *Driver {
	return &Driver{
		db: db,
	}
}
