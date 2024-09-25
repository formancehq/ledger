package storagetesting

import (
	"context"
	"time"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/testing/docker"

	"github.com/formancehq/go-libs/bun/bunconnect"

	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	"github.com/formancehq/ledger/v2/internal/storage/driver"
	"github.com/stretchr/testify/require"
)

func StorageDriver(t docker.T) *driver.Driver {
	pgServer := pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))
	pgDatabase := pgServer.NewDatabase(t)

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
