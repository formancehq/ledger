package sqlstorage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/formancehq/ledger/pkg/opentelemetry"
	"github.com/formancehq/ledger/pkg/storage"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/schema"
	systemstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/system"
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
	name              string
	db                schema.DB
	systemStore       *systemstore.Store
	registeredLedgers map[string]storage.LedgerStore
	lock              sync.Mutex
}

func (d *Driver) GetSystemStore() storage.SystemStore {
	return d.systemStore
}

func (d *Driver) GetLedgerStore(ctx context.Context, name string, create bool) (storage.LedgerStore, bool, error) {
	if name == SystemSchema {
		return nil, false, errors.New("reserved name")
	}
	d.lock.Lock()
	defer d.lock.Unlock()

	ctx, span := opentelemetry.Start(ctx, "Load store")
	defer span.End()

	var (
		created bool
		schema  schema.Schema
	)
	if _, exists := d.registeredLedgers[name]; !exists {
		systemStore := d.systemStore

		exists, err := systemStore.Exists(ctx, name)
		if err != nil {
			return nil, false, errors.Wrap(err, "checking ledger existence")
		}
		if !exists && !create {
			return nil, false, storage.ErrLedgerStoreNotFound
		}

		created, err = systemStore.Register(ctx, name)
		if err != nil {
			return nil, false, errors.Wrap(err, "registering ledger")
		}

		schema, err = d.db.Schema(ctx, name)
		if err != nil {
			return nil, false, errors.Wrap(err, "opening schema")
		}

		if err = schema.Initialize(ctx); err != nil {
			return nil, false, err
		}

		d.registeredLedgers[name] = ledgerstore.NewStore(ctx, schema, func(ctx context.Context) error {
			return schema.Close(context.Background())
		}, func(ctx context.Context) error {
			return d.GetSystemStore().DeleteLedger(ctx, name)
		})
	}

	return d.registeredLedgers[name], created, nil
}

func (d *Driver) Name() string {
	return d.name
}

func (d *Driver) Initialize(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Initialize driver %s", d.name)

	if err := d.db.Initialize(ctx); err != nil {
		return err
	}

	systemSchema, err := d.db.Schema(ctx, SystemSchema)
	if err != nil {
		return err
	}

	if err := systemSchema.Initialize(ctx); err != nil {
		return err
	}

	d.systemStore = systemstore.NewStore(systemSchema)

	if err := d.systemStore.Initialize(ctx); err != nil {
		return err
	}

	return nil
}

func (d *Driver) Close(ctx context.Context) error {
	err := d.systemStore.Close(ctx)
	if err != nil {
		return err
	}
	return d.db.Close(ctx)
}

func NewDriver(name string, db schema.DB) *Driver {
	return &Driver{
		db:                db,
		name:              name,
		registeredLedgers: map[string]storage.LedgerStore{},
	}
}

var _ storage.Driver = (*Driver)(nil)
