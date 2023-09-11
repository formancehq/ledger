package sqlstorage_test

import (
	"context"
	"github.com/numary/ledger/pkg/ledgertesting"
	"os"
	"testing"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testGetBalances(t *testing.T, store *sqlstorage.Store) {
	err := store.Commit(context.Background(), tx1, tx2, tx3)
	require.NoError(t, err)

	t.Run("all accounts", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledger.BalancesQuery{
				PageSize: 10,
			})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		assert.Equal(t, false, cursor.HasMore)
		assert.Equal(t, "", cursor.Previous)
		assert.Equal(t, "", cursor.Next)
		assert.Equal(t, []core.AccountsBalances{
			{
				"world": core.AssetsBalances{
					"USD": core.NewMonetaryInt(-200),
				},
			},
			{
				"users:1": core.AssetsBalances{
					"USD": core.NewMonetaryInt(1),
				},
			},
			{
				"central_bank": core.AssetsBalances{
					"USD": core.NewMonetaryInt(199),
				},
			},
		}, cursor.Data)
	})

	t.Run("on 2 accounts", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledger.BalancesQuery{
				Filters: ledger.BalancesQueryFilters{
					AddressRegexp: []string{"central_bank", "users:1"},
				},
				PageSize: 10,
			})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		assert.Equal(t, false, cursor.HasMore)
		assert.Equal(t, "", cursor.Previous)
		assert.Equal(t, "", cursor.Next)
		assert.Equal(t, []core.AccountsBalances{
			{
				"users:1": core.AssetsBalances{
					"USD": core.NewMonetaryInt(1),
				},
			},
			{
				"central_bank": core.AssetsBalances{
					"USD": core.NewMonetaryInt(199),
				},
			},
		}, cursor.Data)
	})

	t.Run("limit", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledger.BalancesQuery{
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
					"USD": core.NewMonetaryInt(-200),
				},
			},
		}, cursor.Data)
	})

	t.Run("limit and offset", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledger.BalancesQuery{
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
					"USD": core.NewMonetaryInt(1),
				},
			},
		}, cursor.Data)
	})

	t.Run("after", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledger.BalancesQuery{
				PageSize:     10,
				AfterAddress: "world",
			})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		assert.Equal(t, false, cursor.HasMore)
		assert.Equal(t, "", cursor.Previous)
		assert.Equal(t, "", cursor.Next)
		assert.Equal(t, []core.AccountsBalances{
			{
				"users:1": core.AssetsBalances{
					"USD": core.NewMonetaryInt(1),
				},
			},
			{
				"central_bank": core.AssetsBalances{
					"USD": core.NewMonetaryInt(199),
				},
			},
		}, cursor.Data)
	})

	t.Run("after and filter on address", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledger.BalancesQuery{
				PageSize:     10,
				AfterAddress: "world",
				Filters:      ledger.BalancesQueryFilters{AddressRegexp: []string{"users:.+"}},
			})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		assert.Equal(t, false, cursor.HasMore)
		assert.Equal(t, "", cursor.Previous)
		assert.Equal(t, "", cursor.Next)
		assert.Equal(t, []core.AccountsBalances{
			{
				"users:1": core.AssetsBalances{
					"USD": core.NewMonetaryInt(1),
				},
			},
		}, cursor.Data)
	})
}

func testGetBalancesAggregated(t *testing.T, store *sqlstorage.Store) {
	err := store.Commit(context.Background(), tx1, tx2, tx3)
	assert.NoError(t, err)

	q := ledger.AggregatedBalancesQuery{
		PageSize: 10,
	}
	cursor, err := store.GetBalancesAggregated(context.Background(), q)
	assert.NoError(t, err)
	assert.Equal(t, core.AssetsBalances{
		"USD": core.NewMonetaryInt(0),
	}, cursor)
}

func testGetBalancesBigInts(t *testing.T, store *sqlstorage.Store) {

	if os.Getenv("NUMARY_STORAGE_POSTGRES_CONN_STRING") != "" ||
		ledgertesting.StorageDriverName() != "postgres" {
		return
	}

	amount, _ := core.ParseMonetaryInt("5522360000000000000000")
	var txBigInts = core.ExpandedTransaction{
		Transaction: core.Transaction{
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      amount,
						Asset:       "USD",
					},
				},
				Reference: "tx1BigInts",
				Timestamp: now.Add(-3 * time.Hour),
			},
		},
		PostCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: amount,
				},
			},
			"central_bank": {
				"USD": {
					Input:  amount,
					Output: core.NewMonetaryInt(0),
				},
			},
		},
		PreCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(0),
				},
			},
			"central_bank": {
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(0),
				},
			},
		},
	}

	err := store.Commit(context.Background(), txBigInts)
	require.NoError(t, err)

	negativeAmount, _ := core.ParseMonetaryInt("-5522360000000000000000")
	t.Run("all accounts", func(t *testing.T) {
		cursor, err := store.GetBalances(context.Background(),
			ledger.BalancesQuery{
				PageSize: 10,
			})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		assert.Equal(t, false, cursor.HasMore)
		assert.Equal(t, "", cursor.Previous)
		assert.Equal(t, "", cursor.Next)
		assert.Equal(t, []core.AccountsBalances{
			{
				"world": core.AssetsBalances{
					"USD": negativeAmount,
				},
			},
			{
				"central_bank": core.AssetsBalances{
					"USD": amount,
				},
			},
		}, cursor.Data)
	})
}
