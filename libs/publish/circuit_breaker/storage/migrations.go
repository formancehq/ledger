package storage

import (
	"context"
	"fmt"

	"github.com/formancehq/stack/libs/go-libs/migrations"
	"github.com/uptrace/bun"
)

func registerMigrations(migrator *migrations.Migrator, schema string) {
	migrator.RegisterMigrations(
		migrations.Migration{
			Up: func(tx bun.Tx) error {
				_, err := tx.Exec(fmt.Sprintf(`
					CREATE TABLE IF NOT EXISTS %s."circuit_breaker" (
						id bigserial NOT NULL,
						created_at timestamp with time zone NOT NULL,
						topic text NOT NULL,
						data bytea NOT NULL,
						metadata jsonb,
						PRIMARY KEY ("id")
					);

					CREATE INDEX IF NOT EXISTS "circuit_breaker_creation_date_idx" ON %s."circuit_breaker" ("created_at" ASC);
				`, schema, schema))
				return err
			},
		},
	)
}

func Migrate(ctx context.Context, schema string, db *bun.DB) error {
	migrator := migrations.NewMigrator(
		migrations.WithTableName("circuit_breaker_migrations"),
	)

	registerMigrations(migrator, schema)

	return migrator.Up(ctx, db)
}
