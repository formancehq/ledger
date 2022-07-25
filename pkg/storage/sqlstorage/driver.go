package sqlstorage

import (
	"context"

	"github.com/numary/go-libs/sharedlogging"
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

func SQLDriverName(f Flavor) string {
	return sqlDrivers[f].driverName
}

func init() {
	// Default mapping for app driver/sql driver
	UpdateSQLDriverMapping(SQLite, "sqlite3")
	UpdateSQLDriverMapping(PostgreSQL, "pgx")
}

type Driver struct {
	name        string
	db          DB
	systemStore *SystemStore
}

func (d *Driver) GetSystemStore(ctx context.Context) (storage.SystemStore, error) {
	return d.systemStore, nil
}

func (d *Driver) Name() string {
	return d.name
}

func (d *Driver) Initialize(ctx context.Context) (err error) {
	sharedlogging.GetLogger(ctx).Debugf("Initialize driver %s", d.name)

	if err = d.db.initialize(ctx); err != nil {
		return
	}

	systemSchema, err := d.db.Schema(ctx, SystemSchema)
	if err != nil {
		return
	}
	d.systemStore = &SystemStore{systemSchema}

	return d.systemStore.initialize(ctx)
}

func (d *Driver) DeleteLedgerStore(ctx context.Context, name string) error {
	if SystemSchema == name {
		return errors.New("cannot delete system schema")
	}
	schema, err := d.db.Schema(ctx, name)
	if err != nil {
		return err
	}

	err = schema.Delete(ctx)
	if err != nil {
		return err
	}

	return d.systemStore.Delete(ctx, name)
}

func (d *Driver) GetLedgerStore(ctx context.Context, name string, create bool) (storage.LedgerStore, bool, error) {
	if name == SystemSchema {
		return nil, false, errors.New("reserved name")
	}

	exists, err := d.systemStore.Exists(ctx, name)
	if err != nil {
		return nil, false, errors.Wrap(err, "checking ledger existence")
	}
	if !exists && !create {
		return nil, false, errors.New("not exists")
	}

	schema, err := d.db.Schema(ctx, name)
	if err != nil {
		return nil, false, errors.Wrap(err, "opening schema")
	}

	created, err := d.systemStore.Register(ctx, name)
	if err != nil {
		return nil, false, errors.Wrap(err, "registering ledger")
	}

	if err = schema.Initialize(ctx); err != nil {
		return nil, false, err
	}

	store, err := NewStore(schema, func(ctx context.Context) error {
		return schema.close(context.Background())
	})
	if err != nil {
		return nil, false, err
	}
	return store, created, nil
}

func (d *Driver) Close(ctx context.Context) error {
	err := d.systemStore.close(ctx)
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

var _ storage.Driver = &Driver{}
