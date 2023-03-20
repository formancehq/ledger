package cache

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCache(t *testing.T) {
	require.NoError(t, pgtesting.CreatePostgresServer())
	defer func() {
		require.NoError(t, pgtesting.DestroyPostgresServer())
	}()

	driver := ledgertesting.StorageDriver(t)

	require.NoError(t, driver.Initialize(context.Background()))

	manager := NewManager(driver)
	ledger := uuid.NewString()
	cache, err := manager.ForLedger(context.Background(), ledger)
	require.NoError(t, err)

	ledgerStore, _, err := driver.GetLedgerStore(context.Background(), ledger, true)
	require.NoError(t, err)

	_, err = ledgerStore.Initialize(context.Background())
	require.NoError(t, err)

	require.NoError(t, ledgerStore.EnsureAccountExists(context.Background(), "world"))
	require.NoError(t, ledgerStore.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
		"world": {
			"USD/2": {
				Input:  core.NewMonetaryInt(100),
				Output: core.NewMonetaryInt(0),
			},
		},
	}))

	account, err := cache.GetAccountWithVolumes(context.Background(), "world")
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Equal(t, core.AccountWithVolumes{
		Account: core.Account{
			Address:  "world",
			Metadata: core.Metadata{},
		},
		Volumes: map[string]core.Volumes{
			"USD/2": {
				Input:  core.NewMonetaryInt(100),
				Output: core.NewMonetaryInt(0),
			},
		},
		Balances: map[string]*core.MonetaryInt{
			"USD/2": core.NewMonetaryInt(100),
		},
	}, *account)

	volumes := account.Volumes["USD/2"]
	volumes.Output = account.Volumes["USD/2"].Output.Add(core.NewMonetaryInt(10))
	account.Volumes["USD/2"] = volumes
	account.Balances["USD/2"] = core.NewMonetaryInt(90)

	require.NoError(t, ledgerStore.AppendLog(
		context.Background(),
		core.NewTransactionLog(core.Transaction{
			TransactionData: core.TransactionData{
				Postings: []core.Posting{{
					Source:      "world",
					Destination: "bank",
					Amount:      core.NewMonetaryInt(10),
					Asset:       "USD/2",
				}},
			},
		}, nil),
	))
	require.NoError(t, ledgerStore.AppendLog(
		context.Background(),
		core.NewSetMetadataLog(core.Now(), core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeAccount,
			TargetID:   "bank",
			Metadata: core.Metadata{
				"category": "gold",
			},
		}),
	))

	account, err = cache.GetAccountWithVolumes(context.Background(), "bank")
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
		Balances: map[string]*core.MonetaryInt{
			"USD/2": core.NewMonetaryInt(10),
		},
	}, *account)

}
