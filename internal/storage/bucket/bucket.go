package bucket

import (
	"context"
	"database/sql"
	_ "embed"

	"github.com/formancehq/go-libs/platform/postgres"

	"errors"
	"github.com/formancehq/go-libs/migrations"
	"github.com/uptrace/bun"
)

type Bucket struct {
	name string
	db   bun.IDB
}

func (b *Bucket) Migrate(ctx context.Context) error {
	return Migrate(ctx, b.db, b.name)
}

func (b *Bucket) IsUpToDate(ctx context.Context) (bool, error) {
	ret, err := getMigrator(b.name).IsUpToDate(ctx, b.db)
	if err != nil && errors.Is(err, migrations.ErrMissingVersionTable) {
		return false, nil
	}
	return ret, err
}

func (b *Bucket) IsInitialized(ctx context.Context) (bool, error) {
	row := b.db.QueryRowContext(ctx, `
		select schema_name 
		from information_schema.schemata 
		where schema_name = ?;
	`, b.name)
	if row.Err() != nil {
		return false, postgres.ResolveError(row.Err())
	}
	var t string
	if err := row.Scan(&t); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
	}
	return true, nil
}

func New(db bun.IDB, name string) *Bucket {
	return &Bucket{
		db:   db,
		name: name,
	}
}
