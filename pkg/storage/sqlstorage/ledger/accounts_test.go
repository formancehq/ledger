package ledger_test

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	"github.com/stretchr/testify/assert"
)

func TestAccounts(t *testing.T) {
	d, stopFn, err := ledgertesting.StorageDriver(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stopFn()

	assert.NoError(t, d.Initialize(context.Background()))

	defer func(d *sqlstorage.Driver, ctx context.Context) {
		assert.NoError(t, d.Close(ctx))
	}(d, context.Background())

	store, _, err := d.GetLedgerStore(context.Background(), "foo", true)
	assert.NoError(t, err)

	_, err = store.Initialize(context.Background())
	assert.NoError(t, err)

	t.Run("success balance", func(t *testing.T) {
		q := storage.AccountsQuery{
			PageSize: 10,
			Filters: storage.AccountsQueryFilters{
				Balance: "50",
			},
		}

		_, err := store.GetAccounts(context.Background(), q)
		assert.NoError(t, err, "balance filter should not fail")
	})

	t.Run("panic invalid balance", func(t *testing.T) {
		q := storage.AccountsQuery{
			PageSize: 10,
			Filters: storage.AccountsQueryFilters{
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
			q := storage.AccountsQuery{
				PageSize: 10,
				Filters: storage.AccountsQueryFilters{
					Balance:         "50",
					BalanceOperator: "TEST",
				},
			}

			_, _ = store.GetAccounts(context.Background(), q)
		}, "invalid balance operator in storage should panic")
	})

	t.Run("success balance operator", func(t *testing.T) {
		q := storage.AccountsQuery{
			PageSize: 10,
			Filters: storage.AccountsQueryFilters{
				Balance:         "50",
				BalanceOperator: storage.BalanceOperatorGte,
			},
		}

		_, err := store.GetAccounts(context.Background(), q)
		assert.NoError(t, err, "balance operator filter should not fail")
	})
}
