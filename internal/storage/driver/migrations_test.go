//go:build it

package driver_test

import (
	"context"
	"fmt"
	"github.com/formancehq/ledger/internal/storage/driver"
	"testing"

	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/bun/bundebug"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/migrations"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

type HookFn func(ctx context.Context, t *testing.T, db bun.IDB)

type Hook struct {
	Before HookFn
	After  HookFn
}

// todo(libs): export in go libs
type MigrationTest struct {
	migrator *migrations.Migrator
	hooks    map[int][]Hook
	db       bun.IDB
	t        *testing.T
}

func (mt *MigrationTest) Run() {
	ctx := logging.TestingContext()
	i := 0
	for {
		for _, hook := range mt.hooks[i] {
			if hook.Before != nil {
				hook.Before(ctx, mt.t, mt.db)
			}
		}

		more, err := mt.migrator.UpByOne(ctx, mt.db)
		require.NoError(mt.t, err)

		for _, hook := range mt.hooks[i] {
			if hook.After != nil {
				hook.After(ctx, mt.t, mt.db)
			}
		}

		i++

		if !more {
			break
		}
	}
}

func (mt *MigrationTest) Append(i int, hook Hook) {
	mt.hooks[i] = append(mt.hooks[i], hook)
}

func NewMigrationTest(t *testing.T, migrator *migrations.Migrator, db bun.IDB) *MigrationTest {
	return &MigrationTest{
		migrator: migrator,
		hooks:    map[int][]Hook{},
		t:        t,
		db:       db,
	}
}

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

	test := NewMigrationTest(t, driver.GetMigrator(), db)
	test.Append(8, addIdOnLedgerTable)
	test.Run()
}

var addIdOnLedgerTable = Hook{
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
