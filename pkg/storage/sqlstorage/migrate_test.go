package sqlstorage_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/formancehq/ledger/internal/pgtesting"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	"github.com/huandu/go-sqlbuilder"
	"github.com/stretchr/testify/require"
)

func TestMigrates(t *testing.T) {
	pgServer, err := pgtesting.PostgresServer()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := sqlstorage.OpenSQLDB(pgServer.ConnString())
	if err != nil {
		t.Fatal(err)
	}
	db := sqlstorage.NewPostgresDB(sqlDB)

	schema, err := db.Schema(context.Background(), "testing")
	require.NoError(t, err)

	require.NoError(t, schema.Initialize(context.Background()))

	migrations := []sqlstorage.Migration{
		{
			MigrationInfo: core.MigrationInfo{
				Version: "0",
				Name:    "create-schema",
			},
			Handlers: sqlstorage.HandlersByEngine{
				"any": {
					sqlstorage.SQLMigrationFunc(`CREATE TABLE IF NOT EXISTS testing.transactions (
						"id"        integer,
						"reference" varchar,
						"hash"      varchar,

						UNIQUE("id"),
						UNIQUE("reference")
					);`),
					sqlstorage.SQLMigrationFunc(`INSERT INTO testing.transactions VALUES (0, '', '')`),
				},
			},
		},
		{
			MigrationInfo: core.MigrationInfo{
				Version: "1",
				Name:    "update-column",
			},
			Handlers: sqlstorage.HandlersByEngine{
				"postgres": {
					sqlstorage.SQLMigrationFunc(`
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
			Handlers: sqlstorage.HandlersByEngine{
				"any": {
					func(ctx context.Context, schema sqlstorage.Schema, tx *sql.Tx) error {
						ub := sqlbuilder.NewUpdateBuilder()
						sql, args := ub.
							Update(schema.Table("transactions")).
							Set(ub.Assign("timestamp", time.Now())).
							BuildWithFlavor(schema.Flavor())
						_, err := tx.ExecContext(ctx, sql, args...)
						return err
					},
				},
			},
		},
	}

	modified, err := sqlstorage.Migrate(context.Background(), schema, migrations...)
	require.NoError(t, err)
	require.True(t, modified)

}
