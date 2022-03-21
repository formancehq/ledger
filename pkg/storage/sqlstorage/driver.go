package sqlstorage

import (
	"context"
	"errors"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/storage"
	"time"
)

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

type driver struct {
	name         string
	db           DB
	systemSchema Schema
}

func (d *driver) register(ctx context.Context, ledger string) (bool, error) {
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

func (d *driver) exists(ctx context.Context, ledger string) (bool, error) {
	b := sqlbuilder.
		Select("ledger").
		From(d.systemSchema.Table("ledgers"))

	q, args := b.Where(b.E("ledger", ledger)).BuildWithFlavor(d.systemSchema.Flavor())

	ret := d.systemSchema.QueryRowContext(ctx, q, args...)
	if ret.Err() != nil {
		return false, nil
	}
	return true, nil
}

func (d *driver) List(ctx context.Context) ([]string, error) {
	q, args := sqlbuilder.
		Select("ledger").
		From(d.systemSchema.Table("ledgers")).
		BuildWithFlavor(sqlbuilder.Flavor(d.systemSchema.Flavor()))
	rows, err := d.systemSchema.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make([]string, 0)
	for rows.Next() {
		var ledger string
		err := rows.Scan(&ledger)
		if err != nil {
			return nil, err
		}
		res = append(res, ledger)
	}
	return res, nil
}

func (s *driver) Name() string {
	return s.name
}

func (s *driver) Initialize(ctx context.Context) error {
	var err error
	s.systemSchema, err = s.db.Schema(ctx, "_system")
	if err != nil {
		return err
	}
	err = s.systemSchema.Initialize(ctx)
	if err != nil {
		return err
	}
	q, args := sqlbuilder.
		CreateTable(s.systemSchema.Table("ledgers")).
		Define("ledger varchar(255) primary key, addedAt timestamp").
		IfNotExists().
		BuildWithFlavor(s.systemSchema.Flavor())

	_, err = s.systemSchema.ExecContext(ctx, q, args...)
	return err
}

func (s *driver) GetStore(ctx context.Context, name string, create bool) (storage.Store, bool, error) {

	exists, err := s.exists(ctx, name)
	if err != nil {
		return nil, false, err
	}
	if !exists && !create {
		return nil, false, errors.New("not exists")
	}

	schema, err := s.db.Schema(ctx, name)
	if err != nil {
		return nil, false, err
	}
	created, err := s.register(ctx, name)
	if err != nil {
		return nil, false, err
	}
	err = schema.Initialize(ctx)
	if err != nil {
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

func (d *driver) Close(ctx context.Context) error {
	err := d.systemSchema.Close(ctx)
	if err != nil {
		return err
	}
	return d.db.Close(ctx)
}

func NewDriver(name string, db DB) *driver {
	return &driver{
		db:   db,
		name: name,
	}
}
