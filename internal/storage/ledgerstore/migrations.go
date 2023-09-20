package ledgerstore

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/formancehq/stack/libs/go-libs/migrations"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

func (store *Store) getMigrator() *migrations.Migrator {
	migrator := migrations.NewMigrator(migrations.WithSchema(store.Name(), true))
	registerMigrations(migrator, store.name)
	return migrator
}

func (store *Store) Migrate(ctx context.Context) (bool, error) {
	migrator := store.getMigrator()

	if err := migrator.Up(ctx, store.db); err != nil {
		return false, err
	}

	// TODO: Update migrations package to return modifications
	return false, nil
}

func (store *Store) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return store.getMigrator().GetMigrations(ctx, store.db)
}

//go:embed migrations/0-init-schema.sql
var initSchema string

func registerMigrations(migrator *migrations.Migrator, name string) {
	migrator.RegisterMigrations(
		migrations.Migration{
			Name: "Init schema",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {

				needV1Upgrade := false
				row := tx.QueryRowContext(ctx, `select exists (
					select from pg_tables
					where schemaname = ? and tablename  = 'log'
				)`, name)
				if row.Err() != nil {
					return row.Err()
				}
				var ret string
				if err := row.Scan(&ret); err != nil {
					panic(err)
				}
				needV1Upgrade = ret != "false"

				oldSchemaRenamed := fmt.Sprintf(name + oldSchemaRenameSuffix)
				if needV1Upgrade {
					_, err := tx.ExecContext(ctx, fmt.Sprintf(`alter schema "%s" rename to "%s"`, name, oldSchemaRenamed))
					if err != nil {
						return errors.Wrap(err, "renaming old schema")
					}
					_, err = tx.ExecContext(ctx, fmt.Sprintf(`create schema if not exists "%s"`, name))
					if err != nil {
						return errors.Wrap(err, "creating new schema")
					}
				}

				_, err := tx.ExecContext(ctx, initSchema)
				if err != nil {
					return errors.Wrap(err, "initializing new schema")
				}

				if needV1Upgrade {
					if err := migrateLogs(ctx, oldSchemaRenamed, name, tx); err != nil {
						return errors.Wrap(err, "migrating logs")
					}

					_, err = tx.ExecContext(ctx, fmt.Sprintf(`create table goose_db_version as table "%s".goose_db_version with no data`, oldSchemaRenamed))
					if err != nil {
						return err
					}
				}

				return nil
			},
		},
	)
}
