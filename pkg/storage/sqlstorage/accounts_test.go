package sqlstorage

import (
	"context"
	"os"
	"testing"

	"github.com/numary/ledger/pkg/storage"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
)

func TestAccounts(t *testing.T) {
	d := NewDriver("sqlite", &sqliteDB{
		directory: os.TempDir(),
		dbName:    uuid.New(),
	})

	assert.NoError(t, d.Initialize(context.Background()))

	defer func(d *Driver, ctx context.Context) {
		assert.NoError(t, d.Close(ctx))
	}(d, context.Background())

	store, _, err := d.GetStore(context.Background(), "foo", true)
	assert.NoError(t, err)

	_, err = store.Initialize(context.Background())
	assert.NoError(t, err)

	t.Run("success balance", func(t *testing.T) {
		q := storage.AccountsQuery{
			Limit: 10,
			Params: map[string]interface{}{
				"balance": "50",
			},
		}

		_, err := store.GetAccounts(context.Background(), q)
		assert.NoError(t, err, "balance filter should not fail")
	})

	t.Run("panic invalid balance", func(t *testing.T) {
		q := storage.AccountsQuery{
			Limit: 10,
			Params: map[string]interface{}{
				"balance": "toto",
			},
		}

		assert.PanicsWithError(
			t, `invalid balance parameter: strconv.ParseInt: parsing "toto": invalid syntax`,

			func() {
				_, _ = store.GetAccounts(context.Background(), q)
			}, "invalid balance in storage should panic")
	})

	t.Run("panic invalid balance_operator", func(t *testing.T) {
		assert.PanicsWithValue(t, "invalid balance_operator parameter", func() {
			q := storage.AccountsQuery{
				Limit: 10,
				Params: map[string]interface{}{
					"balance":          "50",
					"balance_operator": "toto",
				},
			}

			_, _ = store.GetAccounts(context.Background(), q)
		}, "invalid balance operator in storage should panic")
	})

	t.Run("success balance_operator", func(t *testing.T) {
		q := storage.AccountsQuery{
			Limit: 10,
			Params: map[string]interface{}{
				"balance":          "50",
				"balance_operator": storage.BalanceOperator("gte"),
			},
		}

		_, err := store.GetAccounts(context.Background(), q)
		assert.NoError(t, err, "balance_operator filter should not fail")
	})
}
