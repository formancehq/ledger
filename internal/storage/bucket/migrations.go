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

func GetMigrator(db bun.IDB, name string, options ...migrations.Option) *migrations.Migrator {
	options = append(options, migrations.WithSchema(name))
	migrator := migrations.NewMigrator(db, options...)

	_, transactional := db.(bun.Tx)

	collectOptions := make([]migrations.CollectOption, 0)
	if transactional {
		collectOptions = append(collectOptions, migrations.WithTemplateVars(map[string]any{
			"Transactional": true,
		}))
	}

	allMigrations, err := migrations.CollectMigrations(MigrationsFS, name, collectOptions...)
	if err != nil {
		panic(err)
	}
	migrator.RegisterMigrations(allMigrations...)

	return migrator
}

func migrate(ctx context.Context, tracer trace.Tracer, db bun.IDB, name string, options ...migrations.Option) error {
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
