package ledgerstore

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"sync"
	"time"

	"github.com/formancehq/go-libs/migrations"

	"github.com/formancehq/go-libs/bun/bunconnect"

	"github.com/formancehq/ledger/internal/storage/sqlutils"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

//go:embed migrations
var migrationsDir embed.FS

type singleLedgerOptimization struct {
	mu          sync.RWMutex
	enabled     bool
	ledgerName  string
	lastChecked time.Time
}

type Bucket struct {
	name              string
	db                *bun.DB
	singleLedgerCache *singleLedgerOptimization
}

func (b *Bucket) Name() string {
	return b.name
}

// IsSingleLedger returns true if the bucket optimization is enabled for single-ledger scenarios.
// This allows queries to skip the WHERE ledger = ? clause when there's only one ledger in the bucket.
func (b *Bucket) IsSingleLedger() bool {
	if b.singleLedgerCache == nil {
		return false
	}
	b.singleLedgerCache.mu.RLock()
	defer b.singleLedgerCache.mu.RUnlock()
	return b.singleLedgerCache.enabled
}

// UpdateSingleLedgerState checks if this bucket contains only one ledger and updates the cache accordingly.
// This is called during bucket initialization and when ledgers are created/deleted.
func (b *Bucket) UpdateSingleLedgerState(ctx context.Context, systemDB *bun.DB) error {
	if b.singleLedgerCache == nil {
		b.singleLedgerCache = &singleLedgerOptimization{}
	}

	// Query systemstore to count ledgers in this bucket
	type result struct {
		Ledger string
		Count  int
	}

	var results []result
	err := systemDB.NewSelect().
		Table("_system.ledgers").
		Column("ledger").
		ColumnExpr("COUNT(*) OVER() as count").
		Where("bucket = ?", b.name).
		Limit(2). // Only need to know if count > 1
		Scan(ctx, &results)

	if err != nil {
		return sqlutils.PostgresError(err)
	}

	b.singleLedgerCache.mu.Lock()
	defer b.singleLedgerCache.mu.Unlock()

	if len(results) == 1 && results[0].Count == 1 {
		b.singleLedgerCache.enabled = true
		b.singleLedgerCache.ledgerName = results[0].Ledger
	} else {
		b.singleLedgerCache.enabled = false
		b.singleLedgerCache.ledgerName = ""
	}
	b.singleLedgerCache.lastChecked = time.Now()

	return nil
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
