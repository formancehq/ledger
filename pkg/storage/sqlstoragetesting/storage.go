package sqlstoragetesting

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/ledger/pkg/storage/schema"
	"github.com/formancehq/ledger/pkg/storage/utils"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/stretchr/testify/require"
)

func StorageDriver(t pgtesting.TestingT) *storage.Driver {
	pgServer := pgtesting.NewPostgresDatabase(t)

	db, err := utils.OpenSQLDB(utils.ConnectionOptions{
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

	d := storage.NewDriver("postgres", schema.NewPostgresDB(db), ledgerstore.DefaultStoreConfig)

	require.NoError(t, d.Initialize(context.Background()))

	return d
}
