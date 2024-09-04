//go:build it

package driver_test

import (
	"fmt"
	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/bun/bundebug"
	"github.com/formancehq/go-libs/testing/docker"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/uptrace/bun"
	"testing"

	"github.com/formancehq/go-libs/logging"
	"github.com/stretchr/testify/require"
)

func TestUpgradeAllBuckets(t *testing.T) {
	t.Parallel()

	d := newStorageDriver(t)
	ctx := logging.TestingContext()

	count := 30

	for i := 0; i < count; i++ {
		name := fmt.Sprintf("ledger%d", i)
		_, err := d.CreateBucket(ctx, name)
		require.NoError(t, err)
	}

	require.NoError(t, d.UpgradeAllBuckets(ctx))
}

func newStorageDriver(t docker.T) *driver.Driver {
	t.Helper()

	ctx := logging.TestingContext()
	pgServer := pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))
	pgDatabase := pgServer.NewDatabase(t)

	hooks := make([]bun.QueryHook, 0)
	if testing.Verbose() {
		hooks = append(hooks, bundebug.NewQueryHook())
	}
	db, err := bunconnect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions(), hooks...)
	require.NoError(t, err)

	d := driver.New(db)

	require.NoError(t, d.Initialize(logging.TestingContext()))

	return d
}
