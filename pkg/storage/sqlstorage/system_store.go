package sqlstorage

import (
	"context"
	"database/sql"
	"time"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/storage"
)

type SystemStore struct {
	schema Schema
}

func (s SystemStore) Delete(ctx context.Context, ledger string) error {
	b := sqlbuilder.DeleteFrom(s.schema.Table("ledgers"))
	b = b.Where(b.E("ledger", ledger))
	q, args := b.BuildWithFlavor(s.schema.Flavor())
	_, err := s.schema.ExecContext(ctx, q, args...)
	return err
}

func (s SystemStore) initialize(ctx context.Context) error {
	if err := s.schema.Initialize(ctx); err != nil {
		return err
	}
	q, args := sqlbuilder.
		CreateTable(s.schema.Table("ledgers")).
		Define("ledger varchar(255) primary key, addedAt timestamp").
		IfNotExists().
		BuildWithFlavor(s.schema.Flavor())

	_, err := s.schema.ExecContext(ctx, q, args...)
	return err
}

func (s SystemStore) List(ctx context.Context) ([]string, error) {
	q, args := sqlbuilder.
		Select("ledger").
		From(s.schema.Table("ledgers")).
		BuildWithFlavor(s.schema.Flavor())
	rows, err := s.schema.QueryContext(ctx, q, args...)
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

func (s *SystemStore) Register(ctx context.Context, ledger string) (bool, error) {
	q, args := sqlbuilder.
		InsertInto(s.schema.Table("ledgers")).
		Cols("ledger", "addedAt").
		Values(ledger, time.Now()).
		SQL("ON CONFLICT DO NOTHING").
		BuildWithFlavor(s.schema.Flavor())

	ret, err := s.schema.ExecContext(ctx, q, args...)
	if err != nil {
		return false, err
	}
	affected, err := ret.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (s *SystemStore) Exists(ctx context.Context, ledger string) (bool, error) {
	b := sqlbuilder.
		Select("ledger").
		From(s.schema.Table("ledgers"))

	q, args := b.Where(b.E("ledger", ledger)).BuildWithFlavor(s.schema.Flavor())

	ret := s.schema.QueryRowContext(ctx, q, args...)
	if ret.Err() != nil {
		return false, nil
	}
	var t string
	_ = ret.Scan(&t) // Trigger close
	return true, nil
}

func (s *SystemStore) close(ctx context.Context) error {
	return s.schema.close(ctx)
}

var _ storage.SystemStore = &SystemStore{}
