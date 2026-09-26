//go:build it

package driver_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

// Regression test for https://github.com/formancehq/ledger/issues/2000.
func TestCreateLedgerWithReusedConnection(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	pgDatabase := srv.NewDatabase(t)
	connectionOptions := pgDatabase.ConnectionOptions()
	connectionOptions.MaxOpenConns = 1
	connectionOptions.MaxIdleConns = 1

	db, err := connect.OpenSQLDB(ctx, connectionOptions)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	require.NoError(t, systemstore.Migrate(ctx, db))

	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		systemstore.NewStoreFactory(),
	)

	for _, testCase := range []struct {
		name   string
		bucket string
	}{
		{name: "ledger-a", bucket: "bucket-a"},
		{name: "ledger-b", bucket: "bucket-b"},
	} {
		l, err := ledger.New(testCase.name, ledger.Configuration{Bucket: testCase.bucket})
		require.NoError(t, err)
		_, err = d.CreateLedger(ctx, l)
		require.NoError(t, err)
	}

	for _, name := range []string{"ledger-a", "ledger-b"} {
		l, err := d.GetLedger(ctx, name)
		require.NoError(t, err)
		require.Equal(t, name, l.Name)
	}
}
