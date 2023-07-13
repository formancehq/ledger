package storagetesting

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/driver"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/stretchr/testify/require"
)

func StorageDriver(t pgtesting.TestingT) *driver.Driver {
	pgServer := pgtesting.NewPostgresDatabase(t)

	db, err := storage.OpenSQLDB(storage.ConnectionOptions{
		DatabaseSourceName: pgServer.ConnString(),
		Debug:              testing.Verbose(),
		MaxIdleConns:       40,
		MaxOpenConns:       40,
		ConnMaxIdleTime:    time.Minute,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	d := driver.New("postgres", storage.NewDatabase(db))

	require.NoError(t, d.Initialize(context.Background()))

	return d
}
