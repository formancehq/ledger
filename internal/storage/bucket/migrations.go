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

func GetMigrator(db *bun.DB, name string) *migrations.Migrator {
	migrator := migrations.NewMigrator(db, migrations.WithSchema(name))
	migrations, err := migrations.CollectMigrations(MigrationsFS, name)
	if err != nil {
		panic(err)
	}
	migrator.RegisterMigrations(migrations...)

	return migrator
}

func migrate(ctx context.Context, tracer trace.Tracer, db *bun.DB, name string, minimalVersionReached chan struct{}) error {
	ctx, span := tracer.Start(ctx, "Migrate bucket")
	defer span.End()

	migrator := GetMigrator(db, name)
	version, err := migrator.GetLastVersion(ctx)
	if err != nil {
		if !errors.Is(err, migrations.ErrMissingVersionTable) {
			return err
		}
	}

	if version >= MinimalSchemaVersion {
		close(minimalVersionReached)
	}

	for {
		err := migrator.UpByOne(ctx)
		if err != nil {
			if errors.Is(err, migrations.ErrAlreadyUpToDate) {
				return nil
			}
			return err
		}
		version++

		if version >= MinimalSchemaVersion {
			select {
			case <-minimalVersionReached:
			// already closed
			default:
				close(minimalVersionReached)
			}
		}
	}
}
