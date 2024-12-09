package bucket

import (
	"context"
	"embed"
	"errors"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
)

//go:embed migrations
var MigrationsFS embed.FS

func GetMigrator(db *bun.DB, name string, options ...migrations.Option) *migrations.Migrator {
	options = append(options, migrations.WithSchema(name))
	migrator := migrations.NewMigrator(db, options...)
	migrations, err := migrations.CollectMigrations(MigrationsFS, name)
	if err != nil {
		panic(err)
	}
	migrator.RegisterMigrations(migrations...)

	return migrator
}

func migrate(ctx context.Context, tracer trace.Tracer, db *bun.DB, name string, options ...migrations.Option) error {
	ctx, span := tracer.Start(ctx, "Migrate bucket")
	defer span.End()

	migrator := GetMigrator(db, name, options...)

	for {
		err := migrator.UpByOne(ctx)
		if err != nil {
			if errors.Is(err, migrations.ErrAlreadyUpToDate) {
				return nil
			}
			return err
		}
	}
}
