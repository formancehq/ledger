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

	tx := core.NewTransaction().WithPostings(
		core.NewPosting("world", "users:1", "USD", big.NewInt(1)),
		core.NewPosting("world", "central_bank", "USD", big.NewInt(199)),
	)
	require.NoError(t, insertTransactions(context.Background(), store, *tx))

	t.Run("all accounts", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledgerstore.NewBalancesQuery().WithPageSize(10))
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		require.Equal(t, false, cursor.HasMore)
		require.Equal(t, "", cursor.Previous)
		require.Equal(t, "", cursor.Next)
		require.Equal(t, []core.BalancesByAssetsByAccounts{
			{
				"central_bank": core.BalancesByAssets{
					"USD": big.NewInt(199),
				},
			},
			{
				"users:1": core.BalancesByAssets{
					"USD": big.NewInt(1),
				},
			},
			{
				"world": core.BalancesByAssets{
					"USD": big.NewInt(-200),
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
		require.Equal(t, []core.BalancesByAssetsByAccounts{
			{
				"central_bank": core.BalancesByAssets{
					"USD": big.NewInt(199),
				},
			},
		}, cursor.Data)
	})

	t.Run("after", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledgerstore.NewBalancesQuery().WithPageSize(10).WithAfterAddress("users:1"),
		)
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		require.Equal(t, false, cursor.HasMore)
		require.Equal(t, "", cursor.Previous)
		require.Equal(t, "", cursor.Next)
		require.Equal(t, []core.BalancesByAssetsByAccounts{
			{
				"world": core.BalancesByAssets{
					"USD": big.NewInt(-200),
				},
			},
		}, cursor.Data)
	})

	t.Run("after and filter on address", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledgerstore.NewBalancesQuery().
				WithPageSize(10).
				WithAfterAddress("central_bank").
				WithAddressFilter("users:1"),
		)
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		require.Equal(t, false, cursor.HasMore)
		require.Equal(t, "", cursor.Previous)
		require.Equal(t, "", cursor.Next)
		require.Equal(t, []core.BalancesByAssetsByAccounts{
			{
				"users:1": core.BalancesByAssets{
					"USD": big.NewInt(1),
				},
			},
		}, cursor.Data)
	})
}

func TestGetBalancesAggregated(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	tx := core.NewTransaction().WithPostings(
		core.NewPosting("world", "users:1", "USD", big.NewInt(1)),
		core.NewPosting("world", "users:2", "USD", big.NewInt(199)),
	)
	require.NoError(t, insertTransactions(context.Background(), store, *tx))

	q := ledgerstore.NewBalancesQuery().WithPageSize(10)
	cursor, err := store.GetBalancesAggregated(context.Background(), q)
	require.NoError(t, err)
	require.Equal(t, core.BalancesByAssets{
		"USD": big.NewInt(0),
	}, cursor)

	ret, err := store.GetBalancesAggregated(context.Background(), ledgerstore.NewBalancesQuery().WithPageSize(10).WithAddressFilter("users:"))
	require.NoError(t, err)
	require.Equal(t, core.BalancesByAssets{
		"USD": big.NewInt(200),
	}, ret)
}
