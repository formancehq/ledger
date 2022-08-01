package sqlstorage_test

import (
	"context"
	"testing"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testGetBalances(t *testing.T, store *sqlstorage.Store) {

	err := store.Commit(context.Background(), tx1, tx2, tx3)
	require.NoError(t, err)

	t.Run("all accounts", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			storage.BalancesQuery{
				PageSize: 10,
			})
		assert.NoError(t, err)
		assert.Equal(t, 3, cursor.PageSize)
		assert.Equal(t, false, cursor.HasMore)
		assert.Equal(t, "", cursor.Previous)
		assert.Equal(t, "", cursor.Next)
		assert.Equal(t, []core.AccountsBalances{
			{
				"world": core.AssetsBalances{
					"USD": -200,
				},
			},
			{
				"users:1": core.AssetsBalances{
					"USD": 1,
				},
			},
			{
				"central_bank": core.AssetsBalances{
					"USD": 199,
				},
			},
		}, cursor.Data)
	})

	t.Run("limit", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			storage.BalancesQuery{
				PageSize: 1,
			})
		assert.NoError(t, err)
		assert.Equal(t, 1, cursor.PageSize)
		assert.Equal(t, true, cursor.HasMore)
		assert.Equal(t, "", cursor.Previous)
		assert.NotEqual(t, "", cursor.Next)
		assert.Equal(t, []core.AccountsBalances{
			{
				"world": core.AssetsBalances{
					"USD": -200,
				},
			},
		}, cursor.Data)
	})

	t.Run("limit and offset", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			storage.BalancesQuery{
				PageSize: 1,
				Offset:   1,
			})
		assert.NoError(t, err)
		assert.Equal(t, 1, cursor.PageSize)
		assert.Equal(t, true, cursor.HasMore)
		assert.NotEqual(t, "", cursor.Previous)
		assert.NotEqual(t, "", cursor.Next)
		assert.Equal(t, []core.AccountsBalances{
			{
				"users:1": core.AssetsBalances{
					"USD": 1,
				},
			},
		}, cursor.Data)
	})

	t.Run("after", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			storage.BalancesQuery{
				PageSize:     10,
				AfterAddress: "world",
			})
		assert.NoError(t, err)
		assert.Equal(t, 2, cursor.PageSize)
		assert.Equal(t, false, cursor.HasMore)
		assert.Equal(t, "", cursor.Previous)
		assert.Equal(t, "", cursor.Next)
		assert.Equal(t, []core.AccountsBalances{
			{
				"users:1": core.AssetsBalances{
					"USD": 1,
				},
			},
			{
				"central_bank": core.AssetsBalances{
					"USD": 199,
				},
			},
		}, cursor.Data)
	})

	t.Run("after and filter on address", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			storage.BalancesQuery{
				PageSize:     10,
				AfterAddress: "world",
				Filters:      storage.BalancesQueryFilters{AddressRegexp: "users.+"},
			})
		assert.NoError(t, err)
		assert.Equal(t, 1, cursor.PageSize)
		assert.Equal(t, false, cursor.HasMore)
		assert.Equal(t, "", cursor.Previous)
		assert.Equal(t, "", cursor.Next)
		assert.Equal(t, []core.AccountsBalances{
			{
				"users:1": core.AssetsBalances{
					"USD": 1,
				},
			},
		}, cursor.Data)
	})
}

func testGetBalancesAggregated(t *testing.T, store *sqlstorage.Store) {
	err := store.Commit(context.Background(), tx1, tx2, tx3)
	assert.NoError(t, err)

	q := storage.BalancesQuery{
		PageSize: 10,
	}
	cursor, err := store.GetBalancesAggregated(context.Background(), q)
	assert.NoError(t, err)
	assert.Equal(t, core.AssetsBalances{
		"USD": 0,
	}, cursor)
}
