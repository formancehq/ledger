package bucket

import (
	"context"
	"embed"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
)

//go:embed migrations
var migrationsDir embed.FS

func GetMigrator(name string) *migrations.Migrator {
	migrator := migrations.NewMigrator(migrations.WithSchema(name, true))
	migrator.RegisterMigrationsFromFileSystem(migrationsDir, "migrations")

	return migrator
}

func Migrate(ctx context.Context, tracer trace.Tracer, db bun.IDB, name string) error {
	ctx, span := tracer.Start(ctx, "Migrate bucket")
	defer span.End()

	return GetMigrator(name).Up(ctx, db)
}
