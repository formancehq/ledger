//go:build it

package system

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/bun/bundebug"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/migrations"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"testing"
)

type Hook struct {
	Before func(ctx context.Context, t *testing.T, db bun.IDB)
	After  func(ctx context.Context, t *testing.T, db bun.IDB)
}

// todo(libs): export in go libs
type MigrationTest struct {
	migrator *migrations.Migrator
	hooks    []Hook
	db       bun.IDB
	t        *testing.T
}

func (mt *MigrationTest) Run() {
	ctx := logging.TestingContext()
	i := 0
	for {
		var hook Hook
		if len(mt.hooks) > i {
			hook = mt.hooks[i]
		}
		i++

		if hook.Before != nil {
			hook.Before(ctx, mt.t, mt.db)
		}

		more, err := mt.migrator.UpByOne(ctx, mt.db)
		require.NoError(mt.t, err)

		if hook.After != nil {
			hook.After(ctx, mt.t, mt.db)
		}

		if !more {
			break
		}
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

	test := MigrationTest{
		migrator: getMigrator(),
		hooks: []Hook{
			{},
			{},
			{},
			{},
			{},
			{},
			{},
			{},
			{
				Before: func(ctx context.Context, t *testing.T, db bun.IDB) {
					for i := 0; i < 3; i++ {
						_, err := db.NewInsert().
							Model(&map[string]any{
								"name":    fmt.Sprintf("ledger%d", i),
								"addedat": time.Now().Format(time.RFC3339Nano),
								"bucket":  ledger.DefaultBucket,
								"state":   ledger.StateInUse,
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
						"state":   ledger.StateInUse,
					}
					_, err := db.NewInsert().
						Model(&newLedger).
						TableExpr("_system.ledgers").
						Returning("*").
						Exec(ctx)
					require.NoError(t, err)
					require.Equal(t, "4", newLedger["id"])
				},
			},
		},
		db: db,
		t:  t,
	}
	test.Run()
}
