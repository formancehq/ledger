package sqlstorage_test

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDriver(t *testing.T) {
	d, stopFn, err := ledgertesting.StorageDriver(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stopFn()

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

	_, err = store.Schema().QueryContext(context.Background(), "select * from transactions")
	assert.Error(t, err)
	assert.Equal(t, "sql: database is closed", err.Error())
}

func TestConfiguration(t *testing.T) {
	d, stopFn, err := ledgertesting.StorageDriver(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stopFn()

	require.NoError(t, d.Initialize(context.Background()))

	require.NoError(t, d.GetSystemStore().InsertConfiguration(context.Background(), "foo", "bar"))
	bar, err := d.GetSystemStore().GetConfiguration(context.Background(), "foo")
	require.NoError(t, err)
	require.Equal(t, "bar", bar)
}
