//go:build it

package system

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v3/testing/migrations"
	"github.com/formancehq/ledger/pkg/features"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/bun/bundebug"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

func TestMigrations(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	pgServer := srv.NewDatabase(t)

	hooks := make([]bun.QueryHook, 0)
	if os.Getenv("DEBUG") == "true" {
		hooks = append(hooks, bundebug.NewQueryHook())
	}

	db, err := bunconnect.OpenSQLDB(ctx, pgServer.ConnectionOptions(), hooks...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	test := migrations.NewMigrationTest(t, GetMigrator(db), db)
	test.Append(8, addIdOnLedgerTable)
	test.Append(14, addDefaultFeatures)
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
		require.Equal(t, int64(4), newLedger["id"])
	},
}

var addDefaultFeatures = migrations.Hook{
	After: func(ctx context.Context, t *testing.T, db bun.IDB) {
		type x struct {
			Features map[string]string `bun:"features"`
		}
		model := make([]x, 0)
		err := db.NewSelect().
			ModelTableExpr("_system.ledgers").
			Scan(ctx, &model)
		require.NoError(t, err)

		for _, m := range model {
			require.EqualValues(t, features.DefaultFeatures, m.Features)
		}
	},
}
