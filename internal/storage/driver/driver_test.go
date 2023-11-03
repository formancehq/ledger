package driver_test

import (
	"context"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/google/uuid"

	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/storage/storagetesting"
	"github.com/stretchr/testify/require"
)

func TestConfiguration(t *testing.T) {
	d := storagetesting.StorageDriver(t)
	defer func() {
		_ = d.Close()
	}()

	require.NoError(t, d.GetSystemStore().InsertConfiguration(context.Background(), "foo", "bar"))
	bar, err := d.GetSystemStore().GetConfiguration(context.Background(), "foo")
	require.NoError(t, err)
	require.Equal(t, "bar", bar)
}

func TestConfigurationError(t *testing.T) {
	d := storagetesting.StorageDriver(t)
	defer func() {
		_ = d.Close()
	}()

	_, err := d.GetSystemStore().GetConfiguration(context.Background(), "not_existing")
	require.Error(t, err)
	require.True(t, storage.IsNotFoundError(err))
}

func TestErrorOnOutdatedSchema(t *testing.T) {
	d := storagetesting.StorageDriver(t)
	defer func() {
		_ = d.Close()
	}()

	ctx := logging.TestingContext()

	name := uuid.NewString()
	_, err := d.GetSystemStore().Register(ctx, name)
	require.NoError(t, err)

	store, err := d.GetLedgerStore(ctx, name)
	require.NoError(t, err)

	upToDate, err := store.IsSchemaUpToDate(ctx)
	require.NoError(t, err)
	require.False(t, upToDate)
}
