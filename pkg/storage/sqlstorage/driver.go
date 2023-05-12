package sqlstorage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/formancehq/ledger/pkg/api/idempotency"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/opentelemetry"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/huandu/go-sqlbuilder"
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

// defaultExecutorProvider use the context to register and manage a sql transaction (if the context is mark as transactional)
func defaultExecutorProvider(schema Schema) func(ctx context.Context) (executor, error) {
	return func(ctx context.Context) (executor, error) {
		if !storage.IsTransactional(ctx) {
			return schema, nil
		}

		if storage.IsTransactionRegistered(ctx) {
			return storage.RegisteredTransaction(ctx).(*sql.Tx), nil
		}

		sqlTx, err := schema.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			return nil, err
		}

		storage.RegisterTransaction(ctx, sqlTx, func(ctx context.Context) error {
			return sqlTx.Commit()
		}, func(ctx context.Context) error {
			return sqlTx.Rollback()
		})
		return sqlTx, nil
	}
}

type Driver struct {
	name              string
	db                DB
	systemSchema      Schema
	registeredLedgers map[string]struct{}
	lock              sync.Mutex
}

func (d *Driver) GetSystemStore() storage.SystemStore {
	return &SystemStore{
		systemSchema: d.systemSchema,
	}
}

func (d *Driver) GetLedgerStore(ctx context.Context, name string, create bool) (*Store, bool, error) {
	if name == SystemSchema {
		return nil, false, errors.New("reserved name")
	}
	d.lock.Lock()
	defer d.lock.Unlock()

	ctx, span := opentelemetry.Start(ctx, "Load store")
	defer span.End()

	var (
		created bool
		schema  Schema
		err     error
	)
	if _, exists := d.registeredLedgers[name]; !exists {
		systemStore := &SystemStore{
			systemSchema: d.systemSchema,
		}
		exists, err := systemStore.exists(ctx, name)
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
		d.registeredLedgers[name] = struct{}{}
	} else {
		schema, err = d.db.Schema(ctx, name)
		if err != nil {
			return nil, false, errors.Wrap(err, "opening schema")
		}
	}

	return NewStore(schema, defaultExecutorProvider(schema), func(ctx context.Context) error {
		return schema.Close(context.Background())
	}, func(ctx context.Context) error {
		return d.GetSystemStore().DeleteLedger(ctx, name)
	}), created, nil
}

func (d *Driver) Name() string {
	return d.name
}

func (d *Driver) Initialize(ctx context.Context) (err error) {
	logging.FromContext(ctx).Debugf("Initialize driver %s", d.name)

	if err = d.db.Initialize(ctx); err != nil {
		return
	}

	d.systemSchema, err = d.db.Schema(ctx, SystemSchema)
	if err != nil {
		return
	}

	if err = d.systemSchema.Initialize(ctx); err != nil {
		return
	}

	q, args := sqlbuilder.
		CreateTable(d.systemSchema.Table("ledgers")).
		Define("ledger varchar(255) primary key, addedAt timestamp").
		IfNotExists().
		BuildWithFlavor(d.systemSchema.Flavor())

	_, err = d.systemSchema.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}

	q, args = sqlbuilder.
		CreateTable(d.systemSchema.Table("configuration")).
		Define("key varchar(255) primary key, value text, addedAt timestamp").
		IfNotExists().
		BuildWithFlavor(d.systemSchema.Flavor())
	_, err = d.systemSchema.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}

	return nil
}

func (d *Driver) Close(ctx context.Context) error {
	err := d.systemSchema.Close(ctx)
	if err != nil {
		return err
	}
	return d.db.Close(ctx)
}

func NewDriver(name string, db DB) *Driver {
	return &Driver{
		db:                db,
		name:              name,
		registeredLedgers: map[string]struct{}{},
	}
}

var _ storage.Driver[*Store] = (*Driver)(nil)

type LedgerStorageDriver struct {
	*Driver
}

func (d *LedgerStorageDriver) GetLedgerStore(ctx context.Context, name string, create bool) (ledger.Store, bool, error) {
	return d.Driver.GetLedgerStore(ctx, name, create)
}

var _ storage.Driver[ledger.Store] = (*LedgerStorageDriver)(nil)

func NewLedgerStorageDriverFromRawDriver(driver *Driver) storage.Driver[ledger.Store] {
	return &LedgerStorageDriver{
		Driver: driver,
	}
}

type DefaultStorageDriver struct {
	*Driver
}

func (d *DefaultStorageDriver) GetLedgerStore(ctx context.Context, name string, create bool) (storage.LedgerStore, bool, error) {
	return d.Driver.GetLedgerStore(ctx, name, create)
}

var _ storage.Driver[storage.LedgerStore] = (*DefaultStorageDriver)(nil)

func NewDefaultStorageDriverFromRawDriver(driver *Driver) storage.Driver[storage.LedgerStore] {
	return &DefaultStorageDriver{
		Driver: driver,
	}
}

type IdempotencyStorageDriver struct {
	*Driver
}

func (d *IdempotencyStorageDriver) GetLedgerStore(ctx context.Context, name string, create bool) (idempotency.Store, bool, error) {
	return d.Driver.GetLedgerStore(ctx, name, create)
}

var _ storage.Driver[idempotency.Store] = (*IdempotencyStorageDriver)(nil)

func NewIdempotencyStorageDriverFromRawDriver(driver *Driver) storage.Driver[idempotency.Store] {
	return &IdempotencyStorageDriver{
		Driver: driver,
	}
}
