package sqlstorage

import (
	"context"
	"database/sql"
	"time"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

type SystemStore struct {
	systemSchema Schema
}

func (s *SystemStore) GetConfiguration(ctx context.Context, key string) (string, error) {
	builder := sqlbuilder.
		Select("value").
		From(s.systemSchema.Table("configuration"))
	q, args := builder.
		Where(builder.E("key", key)).
		Limit(1).
		BuildWithFlavor(s.systemSchema.Flavor())

	row := s.systemSchema.QueryRowContext(ctx, q, args...)
	if row.Err() != nil {
		if row.Err() != sql.ErrNoRows {
			return "", nil
		}
	}
	var value string
	if err := row.Scan(&value); err != nil {
		if err == sql.ErrNoRows {
			return "", storage.ErrConfigurationNotFound
		}
		return "", err
	}

	return value, nil
}

func (s SystemStore) InsertConfiguration(ctx context.Context, key, value string) error {
	q, args := sqlbuilder.
		InsertInto(s.systemSchema.Table("configuration")).
		Cols("key", "value", "addedAt").
		Values(key, value, time.Now().UTC().Truncate(time.Second)).
		BuildWithFlavor(s.systemSchema.Flavor())
	_, err := s.systemSchema.ExecContext(ctx, q, args...)
	return errors.Wrap(err, "inserting configuration")
}

func (s SystemStore) ListLedgers(ctx context.Context) ([]string, error) {
	q, args := sqlbuilder.
		Select("ledger").
		From(s.systemSchema.Table("ledgers")).
		BuildWithFlavor(s.systemSchema.Flavor())
	rows, err := s.systemSchema.QueryContext(ctx, q, args...)
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

func (s SystemStore) DeleteLedger(ctx context.Context, name string) error {
	b := sqlbuilder.DeleteFrom(s.systemSchema.Table("ledgers"))
	b = b.Where(b.E("ledger", name))
	q, args := b.BuildWithFlavor(s.systemSchema.Flavor())
	_, err := s.systemSchema.ExecContext(ctx, q, args...)
	return errors.Wrap(err, "delete ledger from system store")
}

func (s *SystemStore) Register(ctx context.Context, ledger string) (bool, error) {
	q, args := sqlbuilder.
		InsertInto(s.systemSchema.Table("ledgers")).
		Cols("ledger", "addedAt").
		Values(ledger, time.Now()).
		SQL("ON CONFLICT DO NOTHING").
		BuildWithFlavor(s.systemSchema.Flavor())

	ret, err := s.systemSchema.ExecContext(ctx, q, args...)
	if err != nil {
		return false, err
	}
	affected, err := ret.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (s *SystemStore) exists(ctx context.Context, ledger string) (bool, error) {
	b := sqlbuilder.
		Select("ledger").
		From(s.systemSchema.Table("ledgers"))

	q, args := b.Where(b.E("ledger", ledger)).BuildWithFlavor(s.systemSchema.Flavor())

	ret := s.systemSchema.QueryRowContext(ctx, q, args...)
	if ret.Err() != nil {
		return false, nil
	}
	var t string
	_ = ret.Scan(&t) // Trigger close
	return true, nil
}

var _ storage.SystemStore = &SystemStore{}
