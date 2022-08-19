package sqlstorage

import (
	"context"
	"database/sql"
	"time"

	"github.com/huandu/go-sqlbuilder"
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
	name         string
	db           DB
	systemSchema Schema
}

func (d *Driver) InsertConfiguration(ctx context.Context, key, value string) error {
	q, args := sqlbuilder.
		InsertInto(d.systemSchema.Table("configuration")).
		Cols("key", "value", "addedAt").
		Values(key, value, time.Now().UTC().Truncate(time.Second)).
		BuildWithFlavor(d.systemSchema.Flavor())
	_, err := d.systemSchema.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	return nil
}

func (d *Driver) Register(ctx context.Context, ledger string) (bool, error) {
	q, args := sqlbuilder.
		InsertInto(d.systemSchema.Table("ledgers")).
		Cols("ledger", "addedAt").
		Values(ledger, time.Now()).
		SQL("ON CONFLICT DO NOTHING").
		BuildWithFlavor(d.systemSchema.Flavor())

	ret, err := d.systemSchema.ExecContext(ctx, q, args...)
	if err != nil {
		return false, err
	}
	affected, err := ret.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (d *Driver) exists(ctx context.Context, ledger string) (bool, error) {
	b := sqlbuilder.
		Select("ledger").
		From(d.systemSchema.Table("ledgers"))

	q, args := b.Where(b.E("ledger", ledger)).BuildWithFlavor(d.systemSchema.Flavor())

	ret := d.systemSchema.QueryRowContext(ctx, q, args...)
	if ret.Err() != nil {
		return false, nil
	}
	var t string
	_ = ret.Scan(&t) // Trigger close
	return true, nil
}

func (d *Driver) List(ctx context.Context) ([]string, error) {
	q, args := sqlbuilder.
		Select("ledger").
		From(d.systemSchema.Table("ledgers")).
		BuildWithFlavor(d.systemSchema.Flavor())
	rows, err := d.systemSchema.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			panic(err)
		}
	}(rows)

	res := make([]string, 0)
	for rows.Next() {
		var ledger string
		if err := rows.Scan(&ledger); err != nil {
			return nil, err
		}
		res = append(res, ledger)
	}
	return res, nil
}

func (d *Driver) Name() string {
	return d.name
}

func (d *Driver) GetConfiguration(ctx context.Context, key string) (string, error) {
	builder := sqlbuilder.
		Select("value").
		From(d.systemSchema.Table("configuration"))
	q, args := builder.
		Where(builder.E("key", key)).
		Limit(1).
		BuildWithFlavor(d.systemSchema.Flavor())

	row := d.systemSchema.QueryRowContext(ctx, q, args...)
	if row.Err() != nil {
		if row.Err() != sql.ErrNoRows {
			return "", nil
		}
	}
	var value string
	if err := row.Scan(&value); err != nil {
		return "", err
	}

	return value, nil
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

func (d *Driver) DeleteStore(ctx context.Context, name string) error {
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

	b := sqlbuilder.DeleteFrom(d.systemSchema.Table("ledgers"))
	b = b.Where(b.E("ledger", name))
	q, args := b.BuildWithFlavor(schema.Flavor())
	_, err = d.systemSchema.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	return nil
}

func (d *Driver) GetStore(ctx context.Context, name string, create bool) (storage.Store, bool, error) {
	if name == SystemSchema {
		return nil, false, errors.New("reserved name")
	}

	exists, err := d.exists(ctx, name)
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

	created, err := d.Register(ctx, name)
	if err != nil {
		return nil, false, errors.Wrap(err, "registering ledger")
	}

	if err = schema.Initialize(ctx); err != nil {
		return nil, false, err
	}

	store, err := NewStore(schema, func(ctx context.Context) error {
		return schema.Close(context.Background())
	})
	if err != nil {
		return nil, false, err
	}
	return store, created, nil
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

var _ storage.Driver = (*Driver)(nil)
