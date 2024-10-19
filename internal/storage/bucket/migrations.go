package bucket

import (
	"context"
	"database/sql"
	"embed"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
)

//go:embed migrations
var migrationsDir embed.FS

func GetMigrator(name string) *migrations.Migrator {
	migrator := migrations.NewMigrator(migrations.WithSchema(name, true))
	migrations, err := migrations.CollectMigrationFiles(migrationsDir, "migrations")
	if err != nil {
		panic(err)
	}

	for ind, migration := range migrations[:12] {
		originalUp := migration.Up
		migration.Up = func(ctx context.Context, db bun.IDB) error {
			return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, "set search_path to '"+name+"'")
				if err != nil {
					return err
				}
				return originalUp(ctx, tx)
			})
		}
		migrations[ind] = migration
	}

	migrator.RegisterMigrations(migrations...)

	return migrator
}

func Migrate(ctx context.Context, tracer trace.Tracer, db bun.IDB, name string) error {
	ctx, span := tracer.Start(ctx, "Migrate bucket")
	defer span.End()

	return GetMigrator(name).Up(ctx, db)
}
