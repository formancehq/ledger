package sqlstorage

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/huandu/go-sqlbuilder"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestMigrates(t *testing.T) {

	tmpDir := os.TempDir()
	db := NewSQLiteDB(tmpDir, uuid.New())
	schema, err := db.Schema(context.Background(), "testing")
	require.NoError(t, err)

	migrations := []Migration{
		{
			MigrationInfo: core.MigrationInfo{
				Version: "0",
				Name:    "create-schema",
			},
			Handlers: HandlersByEngine{
				"any": {
					SQLMigrationFunc(`CREATE TABLE IF NOT EXISTS transactions (
						"id"        integer,
						"reference" varchar,
						"hash"      varchar,

						UNIQUE("id"),
						UNIQUE("reference")
					);`),
					SQLMigrationFunc(`INSERT INTO transactions VALUES (0, "", "")`),
				},
			},
		},
		{
			MigrationInfo: core.MigrationInfo{
				Version: "1",
				Name:    "update-column",
			},
			Handlers: HandlersByEngine{
				"sqlite": {
					SQLMigrationFunc(`
						ALTER TABLE transactions
						ADD COLUMN timestamp date;`),
				},
			},
		},
		{
			MigrationInfo: core.MigrationInfo{
				Version: "2",
				Name:    "init-timestamp",
			},
			Handlers: HandlersByEngine{
				"any": {
					func(ctx context.Context, schema Schema, tx *sql.Tx) error {
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

	modified, err := Migrate(context.Background(), schema, migrations...)
	require.NoError(t, err)
	require.True(t, modified)

}
