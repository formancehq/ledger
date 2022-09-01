package sqlstorage

import (
	"context"
	"database/sql"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/api/idempotency"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

const SystemSchema = "_system"

var sqlDrivers = map[Flavor]struct {
	driverName string
}{}

func UpdateSQLDriverMapping(flavor Flavor, name string) {
	cfg := sqlDrivers[flavor]
	cfg.driverName = name
	sqlDrivers[flavor] = cfg
}

func init() {
	// Default mapping for app driver/sql driver
	UpdateSQLDriverMapping(SQLite, "sqlite3")
	UpdateSQLDriverMapping(PostgreSQL, "pgx")
}

type Driver struct {
	name         string
	db           DB
	systemSchema Schema
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

	schema, err := d.db.Schema(ctx, name)
	if err != nil {
		return nil, false, errors.Wrap(err, "opening schema")
	}

	created, err := systemStore.Register(ctx, name)
	if err != nil {
		return nil, false, errors.Wrap(err, "registering ledger")
	}

	if err = schema.Initialize(ctx); err != nil {
		return nil, false, err
	}

	return NewStore(schema, func(ctx context.Context) (executor, error) {
		var ret executor = schema
		if storage.IsTransactional(ctx) {
			if !storage.IsTransactionRegistered(ctx) {
				sqlTx, err := schema.BeginTx(ctx, &sql.TxOptions{})
				if err != nil {
					return nil, err
				}
				ret = sqlTx
				storage.RegisterTransaction(ctx, sqlTx, func(ctx context.Context) error {
					return sqlTx.Commit()
				}, func(ctx context.Context) error {
					return sqlTx.Rollback()
				})
			} else {
				ret = storage.RegisteredTransaction(ctx).(*sql.Tx)
			}
		}
		return ret, nil
	}, func(ctx context.Context) error {
		return schema.Close(context.Background())
	}, func(ctx context.Context) error {
		return d.GetSystemStore().DeleteLedger(ctx, name)
	}), created, nil
}

func (d *Driver) Name() string {
	return d.name
}

func (d *Driver) Initialize(ctx context.Context) (err error) {
	sharedlogging.GetLogger(ctx).Debugf("Initialize driver %s", d.name)

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
		db:   db,
		name: name,
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
