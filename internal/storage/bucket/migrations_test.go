//go:build it

package bucket_test

import (
	"context"
	"github.com/formancehq/go-libs/testing/migrations"
	"github.com/formancehq/ledger/internal/storage/driver"
	"testing"

	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/bun/bundebug"
	"github.com/formancehq/go-libs/logging"
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
	test.Append(8, removeSequenceOnTransactionTable)
	test.Run()
}

var removeSequenceOnTransactionTable = migrations.Hook{
	Before: func(ctx context.Context, t *testing.T, db bun.IDB) {

	},
	After: func(ctx context.Context, t *testing.T, db bun.IDB) {

	},
}
