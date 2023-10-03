package sqlstorage

import (
	"context"
	"os"
	"testing"

	"github.com/numary/ledger/pkg/ledger"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
)

func TestAccounts(t *testing.T) {
	d := NewDriver("sqlite", &sqliteDB{
		directory: os.TempDir(),
		dbName:    uuid.New(),
	}, false)

	assert.NoError(t, d.Initialize(context.Background()))

	defer func(d *Driver, ctx context.Context) {
		assert.NoError(t, d.Close(ctx))
	}(d, context.Background())

	store, _, err := d.GetLedgerStore(context.Background(), "foo", true)
	assert.NoError(t, err)

	_, err = store.Initialize(context.Background())
	assert.NoError(t, err)

	accountTests(t, store)
}

func TestAccountsMultipleInstance(t *testing.T) {
	d := NewDriver("sqlite", &sqliteDB{
		directory: os.TempDir(),
		dbName:    uuid.New(),
	}, true)

	assert.NoError(t, d.Initialize(context.Background()))

	defer func(d *Driver, ctx context.Context) {
		assert.NoError(t, d.Close(ctx))
	}(d, context.Background())

	store, _, err := d.GetLedgerStore(context.Background(), "foo", true)
	assert.NoError(t, err)

	_, err = store.Initialize(context.Background())
	assert.NoError(t, err)

	accountTests(t, store)
}

func accountTests(t *testing.T, store *Store) {
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
}
