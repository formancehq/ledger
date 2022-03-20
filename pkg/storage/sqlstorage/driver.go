package sqlstorage

import (
	"context"
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
	flavor       Flavor
	db           DB
	systemSchema Schema
}

func (d *driver) register(ctx context.Context, ledger string) error {
	q, args := sqlbuilder.
		InsertInto(d.systemSchema.Table("ledgers")).
		Cols("ledger", "addedAt").
		Values(ledger, time.Now()).
		SQL("ON CONFLICT DO NOTHING").
		BuildWithFlavor(sqlbuilder.Flavor(d.flavor))

	_, err := d.systemSchema.ExecContext(ctx, q, args...)
	return err
}

func (d *driver) List(ctx context.Context) ([]string, error) {
	q, args := sqlbuilder.
		Select("ledger").
		From(d.systemSchema.Table("ledgers")).
		BuildWithFlavor(sqlbuilder.Flavor(d.flavor))
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
		Define("ledger varchar(255), addedAt timestamp").
		IfNotExists().
		BuildWithFlavor(sqlbuilder.Flavor(s.flavor))

	_, err = s.systemSchema.ExecContext(ctx, q, args...)
	return err
}

func (s *driver) NewStore(ctx context.Context, name string) (storage.Store, error) {
	schema, err := s.db.Schema(ctx, name)
	if err != nil {
		return nil, err
	}
	err = s.register(ctx, name)
	if err != nil {
		return nil, err
	}
	err = schema.Initialize(ctx)
	if err != nil {
		return nil, err
	}
	return NewStore(name, sqlbuilder.Flavor(s.flavor), schema, func(ctx context.Context) error {
		return schema.Close(context.Background())
	})
}

func (d *driver) Close(ctx context.Context) error {
	err := d.systemSchema.Close(ctx)
	if err != nil {
		return err
	}
	return d.db.Close(ctx)
}

func NewDriver(name string, flavor Flavor, db DB) *driver {
	return &driver{
		db:     db,
		name:   name,
		flavor: flavor,
	}
}
