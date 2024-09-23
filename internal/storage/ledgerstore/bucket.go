package ledgerstore

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/formancehq/go-libs/migrations"

	"github.com/formancehq/go-libs/bun/bunconnect"

	"github.com/formancehq/ledger/internal/storage/sqlutils"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

//go:embed migrations
var migrationsDir embed.FS

type Bucket struct {
	name string
	db   *bun.DB
}

func (b *Bucket) Name() string {
	return b.name
}

func (b *Bucket) Migrate(ctx context.Context) error {
	return MigrateBucket(ctx, b.db, b.name)
}

func (b *Bucket) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return getBucketMigrator(b.name).GetMigrations(ctx, b.db)
}

func (b *Bucket) IsUpToDate(ctx context.Context) (bool, error) {
	ret, err := getBucketMigrator(b.name).IsUpToDate(ctx, b.db)
	if err != nil && errors.Is(err, migrations.ErrMissingVersionTable) {
		return false, nil
	}
	return ret, err
}

func (b *Bucket) Close() error {
	return b.db.Close()
}

func (b *Bucket) createLedgerStore(name string) (*Store, error) {
	return New(b, name)
}

func (b *Bucket) CreateLedgerStore(name string) (*Store, error) {
	return b.createLedgerStore(name)
}

func (b *Bucket) GetLedgerStore(name string) (*Store, error) {
	return New(b, name)
}

func (b *Bucket) IsInitialized(ctx context.Context) (bool, error) {
	row := b.db.QueryRowContext(ctx, `
		select schema_name 
		from information_schema.schemata 
		where schema_name = ?;
	`, b.name)
	if row.Err() != nil {
		return false, sqlutils.PostgresError(row.Err())
	}
	var t string
	if err := row.Scan(&t); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
	}
	return true, nil
}

func registerMigrations(migrator *migrations.Migrator, name string) {
	ret, err := migrations.CollectMigrationFiles(migrationsDir, "migrations", func(s string) string {
		return s
	})
	if err != nil {
		panic(err)
	}
	initSchema := ret[0]

	// notes(gfyrag): override default schema initialization to handle ledger v1 upgrades
	ret[0] = migrations.Migration{
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

			if err := initSchema.UpWithContext(ctx, tx); err != nil {
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
	}

	migrator.RegisterMigrations(ret...)
}

func ConnectToBucket(ctx context.Context, connectionOptions bunconnect.ConnectionOptions, name string, hooks ...bun.QueryHook) (*Bucket, error) {
	db, err := bunconnect.OpenDBWithSchema(ctx, connectionOptions, name, hooks...)
	if err != nil {
		return nil, sqlutils.PostgresError(err)
	}

	return &Bucket{
		db:   db,
		name: name,
	}, nil
}

func getBucketMigrator(name string) *migrations.Migrator {
	migrator := migrations.NewMigrator(migrations.WithSchema(name, true))
	registerMigrations(migrator, name)
	return migrator
}

func MigrateBucket(ctx context.Context, db bun.IDB, name string) error {
	return getBucketMigrator(name).Up(ctx, db)
}
