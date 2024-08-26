package storagetesting

import (
	"context"
	"time"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/testing/docker"

	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"

	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/stack/libs/go-libs/testing/platform/pgtesting"
	"github.com/stretchr/testify/require"
)

func StorageDriver(t docker.T) *driver.Driver {
	pgServer := pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))
	pgDatabase := pgServer.NewDatabase()

	d := driver.New(bunconnect.ConnectionOptions{
		DatabaseSourceName: pgDatabase.ConnString(),
		MaxIdleConns:       40,
		MaxOpenConns:       40,
		ConnMaxIdleTime:    time.Minute,
	})

	require.NoError(t, d.Initialize(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, d.Close())
	})

	return d
}
