package driver_test

import (
	"fmt"
	"testing"

	"github.com/formancehq/ledger/internal/storage/driver"

	"github.com/formancehq/ledger/internal/storage/sqlutils"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/google/uuid"

	"github.com/formancehq/ledger/internal/storage/storagetesting"
	"github.com/stretchr/testify/require"
)

func TestConfiguration(t *testing.T) {
	t.Parallel()

	d := storagetesting.StorageDriver(t)
	ctx := logging.TestingContext()

	require.NoError(t, d.GetSystemStore().InsertConfiguration(ctx, "foo", "bar"))
	bar, err := d.GetSystemStore().GetConfiguration(ctx, "foo")
	require.NoError(t, err)
	require.Equal(t, "bar", bar)
}

func TestConfigurationError(t *testing.T) {
	t.Parallel()

	d := storagetesting.StorageDriver(t)
	ctx := logging.TestingContext()

	_, err := d.GetSystemStore().GetConfiguration(ctx, "not_existing")
	require.Error(t, err)
	require.True(t, sqlutils.IsNotFoundError(err))
}

func TestErrorOnOutdatedBucket(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	d := storagetesting.StorageDriver(t)

	name := uuid.NewString()

	b, err := d.OpenBucket(name)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = b.Close()
	})

	upToDate, err := b.IsUpToDate(ctx)
	require.NoError(t, err)
	require.False(t, upToDate)
}

func TestGetLedgerFromDefaultBucket(t *testing.T) {
	t.Parallel()

	d := storagetesting.StorageDriver(t)
	ctx := logging.TestingContext()

	name := uuid.NewString()
	_, err := d.CreateLedgerStore(ctx, name, driver.LedgerConfiguration{})
	require.NoError(t, err)
}

func TestGetLedgerFromAlternateBucket(t *testing.T) {
	t.Parallel()

	d := storagetesting.StorageDriver(t)
	ctx := logging.TestingContext()

	ledgerName := "ledger0"
	bucketName := "bucket0"

	_, err := d.CreateLedgerStore(ctx, ledgerName, driver.LedgerConfiguration{
		Bucket: bucketName,
	})
	require.NoError(t, err)
}

func TestUpgradeAllBuckets(t *testing.T) {
	t.Parallel()

	d := storagetesting.StorageDriver(t)
	ctx := logging.TestingContext()

	count := 30

	for i := 0; i < count; i++ {
		name := fmt.Sprintf("ledger%d", i)
		_, err := d.CreateLedgerStore(ctx, name, driver.LedgerConfiguration{
			Bucket: name,
		})
		require.NoError(t, err)
	}

	require.NoError(t, d.UpgradeAllBuckets(ctx))
}
