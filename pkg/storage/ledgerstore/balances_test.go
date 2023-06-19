package ledgerstore_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/stretchr/testify/require"
)

func TestGetBalances(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	require.NoError(t, store.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
		"world": {
			"USD": core.NewEmptyVolumes().WithOutput(big.NewInt(200)),
		},
		"users:1": {
			"USD": core.NewEmptyVolumes().WithInput(big.NewInt(1)),
		},
		"central_bank": {
			"USD": core.NewEmptyVolumes().WithInput(big.NewInt(199)),
		},
	}))

	t.Run("all accounts", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledgerstore.NewBalancesQuery().WithPageSize(10))
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		require.Equal(t, false, cursor.HasMore)
		require.Equal(t, "", cursor.Previous)
		require.Equal(t, "", cursor.Next)
		require.Equal(t, []core.AccountsBalances{
			{
				"world": core.AssetsBalances{
					"USD": big.NewInt(-200),
				},
			},
			{
				"users:1": core.AssetsBalances{
					"USD": big.NewInt(1),
				},
			},
			{
				"central_bank": core.AssetsBalances{
					"USD": big.NewInt(199),
				},
			},
		}, cursor.Data)
	})

	t.Run("limit", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledgerstore.NewBalancesQuery().WithPageSize(1),
		)
		require.NoError(t, err)
		require.Equal(t, 1, cursor.PageSize)
		require.Equal(t, true, cursor.HasMore)
		require.Equal(t, "", cursor.Previous)
		require.NotEqual(t, "", cursor.Next)
		require.Equal(t, []core.AccountsBalances{
			{
				"world": core.AssetsBalances{
					"USD": big.NewInt(-200),
				},
			},
		}, cursor.Data)
	})

	t.Run("after", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledgerstore.NewBalancesQuery().WithPageSize(10).WithAfterAddress("world"),
		)
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		require.Equal(t, false, cursor.HasMore)
		require.Equal(t, "", cursor.Previous)
		require.Equal(t, "", cursor.Next)
		require.Equal(t, []core.AccountsBalances{
			{
				"users:1": core.AssetsBalances{
					"USD": big.NewInt(1),
				},
			},
			{
				"central_bank": core.AssetsBalances{
					"USD": big.NewInt(199),
				},
			},
		}, cursor.Data)
	})

	t.Run("after and filter on address", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledgerstore.NewBalancesQuery().
				WithPageSize(10).
				WithAfterAddress("world").
				WithAddressFilter("users:1"),
		)
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		require.Equal(t, false, cursor.HasMore)
		require.Equal(t, "", cursor.Previous)
		require.Equal(t, "", cursor.Next)
		require.Equal(t, []core.AccountsBalances{
			{
				"users:1": core.AssetsBalances{
					"USD": big.NewInt(1),
				},
			},
		}, cursor.Data)
	})
}

func TestGetBalancesAggregated(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	require.NoError(t, store.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
		"world": {
			"USD": core.NewEmptyVolumes().WithOutput(big.NewInt(200)),
		},
		"users:1": {
			"USD": core.NewEmptyVolumes().WithInput(big.NewInt(1)),
		},
		"central_bank": {
			"USD": core.NewEmptyVolumes().WithInput(big.NewInt(199)),
		},
	}))

	q := ledgerstore.NewBalancesQuery().WithPageSize(10)
	cursor, err := store.GetBalancesAggregated(context.Background(), q)
	require.NoError(t, err)
	require.Equal(t, core.AssetsBalances{
		"USD": big.NewInt(0),
	}, cursor)
}
