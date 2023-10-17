package sqlstorage_test

import (
	"context"
	"testing"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/stretchr/testify/assert"
)

func testAccounts(t *testing.T, store *sqlstorage.Store) {
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
		err := store.Commit(context.Background(), tx1, tx2, tx3, tx4)
		assert.NoError(t, err)

		q := ledger.AccountsQuery{
			PageSize: 10,
			Filters: ledger.AccountsQueryFilters{
				Address: "users:1",
			},
		}

		accounts, err := store.GetAccounts(context.Background(), q)
		assert.NoError(t, err, "balance operator filter should not fail")
		assert.Equal(t, len(accounts.Data), 1)
		assert.Equal(t, accounts.Data[0].Address, core.AccountAddress("users:1"))
	})
}
