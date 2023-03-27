package sqlstorage_test

import (
	"context"
	"os"
	"testing"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/sqlstoragetesting"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(t *testing.M) {
	if err := pgtesting.CreatePostgresServer(); err != nil {
		logging.Error(err)
		os.Exit(1)
	}
	code := t.Run()
	if err := pgtesting.DestroyPostgresServer(); err != nil {
		logging.Error(err)
	}
	os.Exit(code)
}

func TestNewDriver(t *testing.T) {
	d := sqlstoragetesting.StorageDriver(t)

	assert.NoError(t, d.Initialize(context.Background()))

	defer func(d *sqlstorage.Driver, ctx context.Context) {
		assert.NoError(t, d.Close(ctx))
	}(d, context.Background())

	store, _, err := d.GetLedgerStore(context.Background(), "foo", true)
	assert.NoError(t, err)

	_, err = store.Initialize(context.Background())
	assert.NoError(t, err)

	assert.NoError(t, store.Close(context.Background()))
	assert.NoError(t, d.Close(context.Background()))

	_, err = store.(*ledgerstore.Store).Schema().QueryContext(context.Background(), "select * from transactions")
	assert.Error(t, err)
	assert.Equal(t, "sql: database is closed", err.Error())
}

func TestConfiguration(t *testing.T) {
	d := sqlstoragetesting.StorageDriver(t)

	require.NoError(t, d.Initialize(context.Background()))

	require.NoError(t, d.GetSystemStore().InsertConfiguration(context.Background(), "foo", "bar"))
	bar, err := d.GetSystemStore().GetConfiguration(context.Background(), "foo")
	require.NoError(t, err)
	require.Equal(t, "bar", bar)
}

func TestConfigurationError(t *testing.T) {
	d := sqlstoragetesting.StorageDriver(t)

	require.NoError(t, d.Initialize(context.Background()))

	_, err := d.GetSystemStore().GetConfiguration(context.Background(), "not_existing")
	require.Error(t, err)
	require.True(t, storage.IsNotFoundError(err))
}
