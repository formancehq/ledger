package storagetesting

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/ledger/internal/storage/sqlutils"

	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/stretchr/testify/require"
)

func StorageDriver(t pgtesting.TestingT) *driver.Driver {
	pgServer := pgtesting.NewPostgresDatabase(t)

	d := driver.New(sqlutils.ConnectionOptions{
		DatabaseSourceName: pgServer.ConnString(),
		Debug:              testing.Verbose(),
		MaxIdleConns:       40,
		MaxOpenConns:       40,
		ConnMaxIdleTime:    time.Minute,
	})

	require.NoError(t, d.Initialize(context.Background()))

	return d
}
