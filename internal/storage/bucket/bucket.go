package bucket

import (
	"context"
	_ "embed"
	"go.opentelemetry.io/otel/trace"

	"errors"
	"github.com/formancehq/go-libs/migrations"
	"github.com/uptrace/bun"
)

type Bucket struct {
	name string
	db   bun.IDB
}

func (b *Bucket) Migrate(ctx context.Context, tracer trace.Tracer) error {
	return Migrate(ctx, tracer, b.db, b.name)
}

func (b *Bucket) IsUpToDate(ctx context.Context) (bool, error) {
	ret, err := GetMigrator(b.name).IsUpToDate(ctx, b.db)
	if err != nil && errors.Is(err, migrations.ErrMissingVersionTable) {
		return false, nil
	}
	return ret, err
}

func New(db bun.IDB, name string) *Bucket {
	return &Bucket{
		db:   db,
		name: name,
	}
}
