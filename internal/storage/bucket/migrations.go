package bucket

import (
	"context"
	"embed"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
)

//go:embed migrations
var MigrationsFS embed.FS

func GetMigrator(db *bun.DB, name string) *migrations.Migrator {
	migrator := migrations.NewMigrator(db, migrations.WithSchema(name, true))
	migrations, err := migrations.CollectMigrations(MigrationsFS, name)
	if err != nil {
		panic(err)
	}
	migrator.RegisterMigrations(migrations...)

	return migrator
}

func migrate(ctx context.Context, tracer trace.Tracer, db *bun.DB, name string) error {
	ctx, span := tracer.Start(ctx, "Migrate bucket")
	defer span.End()

	return GetMigrator(db, name).Up(ctx)
}
