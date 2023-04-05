package ledger_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/sqlstoragetesting"
	"github.com/stretchr/testify/require"
)

func TestVolumes(t *testing.T) {
	d := sqlstoragetesting.StorageDriver(t)

	require.NoError(t, d.Initialize(context.Background()))

	defer func(d *sqlstorage.Driver, ctx context.Context) {
		require.NoError(t, d.Close(ctx))
	}(d, context.Background())

	store, _, err := d.GetLedgerStore(context.Background(), "foo", true)
	require.NoError(t, err)

	_, err = store.Initialize(context.Background())
	require.NoError(t, err)

	t.Run("success update volumes", func(t *testing.T) {
		foo := core.AssetsVolumes{
			"bar": {
				Input:  big.NewInt(1),
				Output: big.NewInt(2),
			},
		}

		foo2 := core.AssetsVolumes{
			"bar2": {
				Input:  big.NewInt(3),
				Output: big.NewInt(4),
			},
		}

		volumes := core.AccountsAssetsVolumes{
			"foo":  foo,
			"foo2": foo2,
		}

		err := store.UpdateVolumes(context.Background(), volumes)
		require.NoError(t, err, "update volumes should not fail")

		assetVolumes, err := store.GetAssetsVolumes(context.Background(), "foo")
		require.NoError(t, err, "get asset volumes should not fail")
		require.Equal(t, foo, assetVolumes, "asset volumes should be equal")

		assetVolumes, err = store.GetAssetsVolumes(context.Background(), "foo2")
		require.NoError(t, err, "get asset volumes should not fail")
		require.Equal(t, foo2, assetVolumes, "asset volumes should be equal")
	})

	t.Run("success update same volume", func(t *testing.T) {
		foo := core.AssetsVolumes{
			"bar": {
				Input:  big.NewInt(1),
				Output: big.NewInt(2),
			},
		}

		foo2 := core.AssetsVolumes{
			"bar": {
				Input:  big.NewInt(3),
				Output: big.NewInt(4),
			},
		}

		volumes := []core.AccountsAssetsVolumes{
			{
				"foo": foo,
			},
			{
				"foo": foo2,
			},
		}

		err := store.UpdateVolumes(context.Background(), volumes...)
		require.NoError(t, err, "update volumes should not fail")

		assetVolumes, err := store.GetAssetsVolumes(context.Background(), "foo")
		require.NoError(t, err, "get asset volumes should not fail")
		require.Equal(t, foo2, assetVolumes, "asset volumes should be equal")
	})
}
