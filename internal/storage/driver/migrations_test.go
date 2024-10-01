//go:build it

package driver_test

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/testing/migrations"
	"github.com/formancehq/ledger/internal/storage/driver"
	"testing"

	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/bun/bundebug"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

func TestMigrations(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	pgServer := srv.NewDatabase(t)

	hooks := make([]bun.QueryHook, 0)
	if testing.Verbose() {
		hooks = append(hooks, bundebug.NewQueryHook())
	}

	db, err := bunconnect.OpenSQLDB(ctx, pgServer.ConnectionOptions(), hooks...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	test := migrations.NewMigrationTest(t, driver.GetMigrator(), db)
	test.Append(8, addIdOnLedgerTable)
	test.Run()
}

var addIdOnLedgerTable = migrations.Hook{
	Before: func(ctx context.Context, t *testing.T, db bun.IDB) {
		for i := 0; i < 3; i++ {
			_, err := db.NewInsert().
				Model(&map[string]any{
					"name":    fmt.Sprintf("ledger%d", i),
					"addedat": time.Now().Format(time.RFC3339Nano),
					"bucket":  ledger.DefaultBucket,
				}).
				TableExpr("_system.ledgers").
				Exec(ctx)
			require.NoError(t, err)
		}
	},
	After: func(ctx context.Context, t *testing.T, db bun.IDB) {

		for i := 0; i < 3; i++ {
			model := make(map[string]any)
			err := db.NewSelect().
				Model(&model).
				ModelTableExpr("_system.ledgers").
				Where("id = ?", fmt.Sprint(i+1)).
				Scan(ctx)
			require.NoError(t, err)
		}

		newLedger := map[string]any{
			"name":    "ledger3",
			"addedat": time.Now().Format(time.RFC3339Nano),
			"bucket":  ledger.DefaultBucket,
		}
		_, err := db.NewInsert().
			Model(&newLedger).
			TableExpr("_system.ledgers").
			Returning("*").
			Exec(ctx)
		require.NoError(t, err)
		require.Equal(t, "4", newLedger["id"])
	},
}
