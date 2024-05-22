package storagetesting

import (
	"context"
	"time"

	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"

	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/stretchr/testify/require"
)

func StorageDriver(t pgtesting.TestingT) *driver.Driver {
	pgDatabase := pgtesting.NewPostgresDatabase(t)

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
