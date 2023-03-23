package ledger_test

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccounts(t *testing.T) {
	d := ledgertesting.StorageDriver(t)

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

func testComputeAccount(t *testing.T, store storage.LedgerStore) {
	require.NoError(t, store.EnsureAccountExists(context.Background(), "world"))
	require.NoError(t, store.EnsureAccountExists(context.Background(), "bank"))
	require.NoError(t, store.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
		"world": {
			"USD/2": {
				Input:  core.NewMonetaryInt(100),
				Output: core.NewMonetaryInt(0),
			},
		},
	}))
	log := core.NewTransactionLog(core.Transaction{
		TransactionData: core.TransactionData{
			Postings: []core.Posting{{
				Source:      "world",
				Destination: "bank",
				Amount:      core.NewMonetaryInt(10),
				Asset:       "USD/2",
			}},
		},
	}, nil)
	require.NoError(t, store.AppendLog(context.Background(), &log))

	log2 := core.NewSetMetadataLog(core.Now(), core.SetMetadataLogPayload{
		TargetType: core.MetaTargetTypeAccount,
		TargetID:   "bank",
		Metadata: core.Metadata{
			"category": "gold",
		},
	})
	require.NoError(t, store.AppendLog(context.Background(), &log2))

	account, err := store.ComputeAccount(context.Background(), "bank")
	require.NoError(t, err)
	require.NotNil(t, account)

	require.Equal(t, core.AccountWithVolumes{
		Account: core.Account{
			Address: "bank",
			Metadata: core.Metadata{
				"category": "gold",
			},
		},
		Volumes: map[string]core.Volumes{
			"USD/2": {
				Input:  core.NewMonetaryInt(10),
				Output: core.NewMonetaryInt(0),
			},
		},
	}, *account)

}
