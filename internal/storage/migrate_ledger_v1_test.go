package storage_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/testing/docker"

	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/systemstore"
	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/testing/platform/pgtesting"
	"github.com/stretchr/testify/require"
)

func TestMigrateLedgerV1(t *testing.T) {
	dockerPool := docker.NewPool(t, logging.Testing())
	srv := pgtesting.CreatePostgresServer(t, dockerPool)

	db, err := sql.Open("postgres", srv.GetDSN())
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join("testdata", "v1-dump.sql"))
	require.NoError(t, err)

	_, err = db.Exec(string(data))
	require.NoError(t, err)

	ctx := logging.TestingContext()

	d := driver.New(bunconnect.ConnectionOptions{
		DatabaseSourceName: srv.GetDSN(),
	})
	require.NoError(t, d.Initialize(ctx))

	ledgers, err := d.GetSystemStore().ListLedgers(ctx, systemstore.ListLedgersQuery{})
	require.NoError(t, err)

	for _, ledger := range ledgers.Data {
		require.NotEmpty(t, ledger.Bucket)
		require.Equal(t, ledger.Name, ledger.Bucket)

		bucket, err := d.OpenBucket(ctx, ledger.Bucket)
		require.NoError(t, err)
		require.NoError(t, bucket.Migrate(ctx))

		store, err := bucket.GetLedgerStore(ledger.Name)
		require.NoError(t, err)

		txs, err := store.GetTransactions(ctx, ledgerstore.NewGetTransactionsQuery(ledgerstore.PaginatedQueryOptions[ledgerstore.PITFilterWithVolumes]{}))
		require.NoError(t, err)
		require.NotEmpty(t, txs)

		accounts, err := store.GetAccountsWithVolumes(ctx, ledgerstore.NewGetAccountsQuery(ledgerstore.PaginatedQueryOptions[ledgerstore.PITFilterWithVolumes]{}))
		require.NoError(t, err)
		require.NotEmpty(t, accounts)
	}
}
