package migrations_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/migrations"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/schema"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/utils"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/psanford/memfs"
	"github.com/stretchr/testify/require"
)

func TestMain(t *testing.M) {
	if err := pgtesting.CreatePostgresServer(); err != nil {
		logging.Error(err)
		os.Exit(1)
	}
	code := t.Run()
	if err := pgtesting.DestroyPostgresServer(); err != nil {
		logging.Error(err)
	}
	os.Exit(code)
}

func TestCollectMigrations(t *testing.T) {
	migrations.PurgeGoMigrations()

	mfs := memfs.New()
	require.NoError(t, mfs.MkdirAll("migrates/0-first-migration", 0666))
	require.NoError(t, mfs.WriteFile("migrates/0-first-migration/postgres.sql", []byte(`
		--statement
		NO SQL;
	`), 0666))
	require.NoError(t, mfs.WriteFile("migrates/0-first-migration/sqlite.go", []byte{}, 0666))
	require.NoError(t, mfs.MkdirAll("migrates/1-second-migration", 0666))
	require.NoError(t, mfs.WriteFile("migrates/1-second-migration/any.sql", []byte(`
		--statement
		NO SQL;
	`), 0666))

	migrations.RegisterGoMigrationFromFilename("migrates/0-first-migration/sqlite.go", func(ctx context.Context, schema schema.Schema, tx *schema.Tx) error {
		return nil
	})

	migrations, err := migrations.CollectMigrationFiles(mfs)
	require.NoError(t, err)
	require.Len(t, migrations, 2)

	require.Equal(t, "0", migrations[0].Version)
	require.Equal(t, "first-migration", migrations[0].Name)
	require.Len(t, migrations[0].Handlers, 2)
	require.Len(t, migrations[0].Handlers["sqlite"], 1)
	require.Len(t, migrations[0].Handlers["postgres"], 1)

	require.Equal(t, "1", migrations[1].Version)
	require.Equal(t, "second-migration", migrations[1].Name)
	require.Len(t, migrations[1].Handlers, 1)
	require.Len(t, migrations[1].Handlers["any"], 1)
}

func TestMigrationsOrders(t *testing.T) {
	mfs := memfs.New()
	for i := 0; i < 1000; i++ {
		dir := fmt.Sprintf("migrates/%d-migration", i)
		require.NoError(t, mfs.MkdirAll(dir, 0666))
		require.NoError(t, mfs.WriteFile(fmt.Sprintf("%s/postgres.sql", dir), []byte(`
		--statement
		NO SQL;
	`), 0666))
	}

	migrations, err := migrations.CollectMigrationFiles(mfs)
	require.NoError(t, err)
	for i, m := range migrations {
		require.Equal(t, fmt.Sprintf("%d", i), m.Version)
	}
}

func TestMigrates(t *testing.T) {
	pgServer := pgtesting.NewPostgresDatabase(t)
	sqlDB, err := utils.OpenSQLDB(utils.ConnectionOptions{
		DatabaseSourceName: pgServer.ConnString(),
		Debug:              testing.Verbose(),
	})
	if err != nil {
		t.Fatal(err)
	}
	db := schema.NewPostgresDB(sqlDB)

	s, err := db.Schema(context.Background(), "testing")
	require.NoError(t, err)

	require.NoError(t, s.Initialize(context.Background()))

	ms := []migrations.Migration{
		{
			MigrationInfo: core.MigrationInfo{
				Version: "0",
				Name:    "create-schema",
			},
			Handlers: migrations.HandlersByEngine{
				"any": {
					migrations.SQLMigrationFunc(`CREATE TABLE IF NOT EXISTS testing.transactions (
						"id"        integer,
						"reference" varchar,
						"hash"      varchar,

						UNIQUE("id"),
						UNIQUE("reference")
					);`),
					migrations.SQLMigrationFunc(`INSERT INTO testing.transactions VALUES (0, '', '')`),
				},
			},
		},
		{
			MigrationInfo: core.MigrationInfo{
				Version: "1",
				Name:    "update-column",
			},
			Handlers: migrations.HandlersByEngine{
				"postgres": {
					migrations.SQLMigrationFunc(`
						ALTER TABLE testing.transactions
						ADD COLUMN timestamp date;`),
				},
			},
		},
		{
			MigrationInfo: core.MigrationInfo{
				Version: "2",
				Name:    "init-timestamp",
			},
			Handlers: migrations.HandlersByEngine{
				"any": {
					func(ctx context.Context, schema schema.Schema, tx *schema.Tx) error {
						sb := s.NewUpdate(ledgerstore.TransactionsTableName).
							Model((*ledgerstore.Transaction)(nil)).
							Set("timestamp = ?", core.Now()).
							Where("TRUE")

						_, err := tx.ExecContext(ctx, sb.String())

						return err
					},
				},
			},
		},
	}

	modified, err := migrations.Migrate(context.Background(), s, ms...)
	require.NoError(t, err)
	require.True(t, modified)

}

func TestRegister(t *testing.T) {
	fn := func(ctx context.Context, schema schema.Schema, tx *schema.Tx) error {
		return nil
	}

	migrations.PurgeGoMigrations()
	migrations.RegisterGoMigrationFromFilename(filepath.Join("XXX", "0-init-schema", "any.go"), fn)
	require.Len(t, migrations.RegisteredGoMigrations, 1)
	migrations.PurgeGoMigrations()
}
