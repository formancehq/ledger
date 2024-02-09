package sqlstorage_test

import (
	"context"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func testAccounts(t *testing.T, store *sqlstorage.Store) {

	err := store.Commit(context.Background(),
		core.ExpandedTransaction{
			Transaction: core.Transaction{
				TransactionData: core.TransactionData{
					Postings: []core.Posting{
						{
							Source:      "world",
							Destination: "us_bank",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD/2",
						},
						{
							Source:      "world",
							Destination: "eu_bank",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "EUR/2",
						},
					},
				},
			},
			PreCommitVolumes: map[string]core.AssetsVolumes{
				"world": map[string]core.Volumes{
					"USD/2": {},
					"EUR/2": {},
				},
				"us_bank": map[string]core.Volumes{
					"USD/2": {},
				},
				"eu_bank": map[string]core.Volumes{
					"EUR/2": {},
				},
			},
			PostCommitVolumes: map[string]core.AssetsVolumes{
				"world": map[string]core.Volumes{
					"USD/2": {
						Output: core.NewMonetaryInt(100),
					},
					"EUR/2": {
						Output: core.NewMonetaryInt(100),
					},
				},
				"us_bank": map[string]core.Volumes{
					"USD/2": {
						Input: core.NewMonetaryInt(100),
					},
				},
				"eu_bank": map[string]core.Volumes{
					"EUR/2": {
						Input: core.NewMonetaryInt(100),
					},
				},
			},
		},
	)
	require.NoError(t, err)

	t.Run("success balance", func(t *testing.T) {
		q := ledger.AccountsQuery{
			PageSize: 10,
			Filters: ledger.AccountsQueryFilters{
				Balance: "50",
			},
		}

		_, err := store.GetAccounts(context.Background(), q)
		assert.NoError(t, err, "balance filter should not fail")
	})
	t.Run("filter balance when multiple assets match", func(t *testing.T) {
		q := ledger.AccountsQuery{
			PageSize: 10,
			Filters: ledger.AccountsQueryFilters{
				Balance:         "0",
				BalanceOperator: "lt",
			},
		}

		accounts, err := store.GetAccounts(context.Background(), q)
		require.NoError(t, err, "balance filter should not fail")
		require.Len(t, accounts.Data, 1)
		require.EqualValues(t, "world", accounts.Data[0].Address)
	})

	t.Run("panic invalid balance", func(t *testing.T) {
		q := ledger.AccountsQuery{
			PageSize: 10,
			Filters: ledger.AccountsQueryFilters{
				Balance: "TEST",
			},
		}

		assert.PanicsWithError(
			t, `invalid balance parameter: strconv.ParseInt: parsing "TEST": invalid syntax`,

			func() {
				_, _ = store.GetAccounts(context.Background(), q)
			}, "invalid balance in storage should panic")
	})

	t.Run("panic invalid balance operator", func(t *testing.T) {
		assert.PanicsWithValue(t, "invalid balance operator parameter", func() {
			q := ledger.AccountsQuery{
				PageSize: 10,
				Filters: ledger.AccountsQueryFilters{
					Balance:         "50",
					BalanceOperator: "TEST",
				},
			}

			_, _ = store.GetAccounts(context.Background(), q)
		}, "invalid balance operator in storage should panic")
	})

	t.Run("success balance operator", func(t *testing.T) {
		q := ledger.AccountsQuery{
			PageSize: 10,
			Filters: ledger.AccountsQueryFilters{
				Balance:         "50",
				BalanceOperator: ledger.BalanceOperatorGte,
			},
		}

		_, err := store.GetAccounts(context.Background(), q)
		assert.NoError(t, err, "balance operator filter should not fail")
	})

	t.Run("success get accounts with address filters", func(t *testing.T) {
		q := ledger.AccountsQuery{
			PageSize: 10,
			Filters: ledger.AccountsQueryFilters{
				Address: "us_bank",
			},
		}

		accounts, err := store.GetAccounts(context.Background(), q)
		assert.NoError(t, err, "balance operator filter should not fail")
		assert.Equal(t, len(accounts.Data), 1)
		assert.Equal(t, accounts.Data[0].Address, core.AccountAddress("us_bank"))
	})
}
