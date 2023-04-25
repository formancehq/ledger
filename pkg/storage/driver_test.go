package storage_test

import (
	"context"
	"os"
	"testing"

	"github.com/formancehq/ledger/pkg/storage/errors"
	"github.com/formancehq/ledger/pkg/storage/sqlstoragetesting"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
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

func TestConfiguration(t *testing.T) {
	d := sqlstoragetesting.StorageDriver(t)

	require.NoError(t, d.GetSystemStore().InsertConfiguration(context.Background(), "foo", "bar"))
	bar, err := d.GetSystemStore().GetConfiguration(context.Background(), "foo")
	require.NoError(t, err)
	require.Equal(t, "bar", bar)
}

func TestConfigurationError(t *testing.T) {
	d := sqlstoragetesting.StorageDriver(t)

	_, err := d.GetSystemStore().GetConfiguration(context.Background(), "not_existing")
	require.Error(t, err)
	require.True(t, errors.IsNotFoundError(err))
}
