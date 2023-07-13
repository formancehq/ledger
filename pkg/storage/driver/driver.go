package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	systemstore "github.com/formancehq/ledger/pkg/storage/systemstore"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
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
	name        string
	db          *storage.Database
	systemStore *systemstore.Store
	lock        sync.Mutex
}

func (d *Driver) GetSystemStore() *systemstore.Store {
	return d.systemStore
}

func (d *Driver) newStore(ctx context.Context, name string) (*ledgerstore.Store, error) {
	schema, err := d.db.Schema(name)
	if err != nil {
		return nil, errors.Wrap(err, "opening schema")
	}

	if err = schema.Create(ctx); err != nil {
		return nil, err
	}

	store, err := ledgerstore.New(schema, func(ctx context.Context) error {
		return d.GetSystemStore().DeleteLedger(ctx, name)
	})
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (d *Driver) CreateLedgerStore(ctx context.Context, name string) (*ledgerstore.Store, error) {
	if name == SystemSchema {
		return nil, errors.New("reserved name")
	}
	d.lock.Lock()
	defer d.lock.Unlock()

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

	store, err := d.newStore(ctx, name)
	if err != nil {
		return nil, err
	}

	_, err = store.Migrate(ctx)

	return store, err
}

func (d *Driver) GetLedgerStore(ctx context.Context, name string) (*ledgerstore.Store, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	exists, err := d.systemStore.Exists(ctx, name)
	if err != nil {
		return nil, errors.Wrap(err, "checking ledger existence")
	}
	if !exists {
		return nil, storage.ErrStoreNotFound
	}

	return d.newStore(ctx, name)
}

func (d *Driver) Name() string {
	return d.name
}

func (d *Driver) Initialize(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Initialize driver %s", d.name)

	if err := d.db.Initialize(ctx); err != nil {
		return err
	}

	systemSchema, err := d.db.Schema(SystemSchema)
	if err != nil {
		return err
	}

	if err := systemSchema.Create(ctx); err != nil {
		return err
	}

	d.systemStore = systemstore.NewStore(systemSchema)

	if err := d.systemStore.Initialize(ctx); err != nil {
		return err
	}

	return nil
}

func (d *Driver) Close(ctx context.Context) error {
	return d.db.Close(ctx)
}

func New(name string, db *storage.Database) *Driver {
	return &Driver{
		db:   db,
		name: name,
	}
}
