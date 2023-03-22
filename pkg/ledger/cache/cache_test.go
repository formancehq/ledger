package cache

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestComputeAccountFromLogs(t *testing.T) {

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
	require.NoError(t, ledgerStore.AppendLog(context.Background(), &log))

	log2 := core.NewSetMetadataLog(core.Now(), core.SetMetadataLogPayload{
		TargetType: core.MetaTargetTypeAccount,
		TargetID:   "bank",
		Metadata: core.Metadata{
			"category": "gold",
		},
	})
	require.NoError(t, ledgerStore.AppendLog(context.Background(), &log2))

	account, err := cache.GetAccountWithVolumes(context.Background(), "bank")
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

func TestRetrieveValueFromCache(t *testing.T) {

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
	account, err := cache.GetAccountWithVolumes(context.Background(), "world")
	require.NoError(t, err)
	require.Equal(t, core.AccountWithVolumes{
		Account: core.Account{
			Address:  "world",
			Metadata: core.Metadata{},
		},
		Volumes: map[string]core.Volumes{},
	}, *account)

	cache.UpdateVolumeWithTX(core.NewTransaction().WithPostings(
		core.NewPosting("world", "bank", "USD", core.NewMonetaryInt(100)),
	))
	account, err = cache.GetAccountWithVolumes(context.Background(), "world")
	require.NoError(t, err)
	require.EqualValues(t, core.AccountWithVolumes{
		Account: core.Account{
			Address:  "world",
			Metadata: core.Metadata{},
		},
		Volumes: map[string]core.Volumes{
			"USD": core.NewEmptyVolumes().WithOutput(core.NewMonetaryInt(100)),
		},
	}, *account)
}

func TestUpdateVolumes(t *testing.T) {

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
	worldAccount, err := cache.GetAccountWithVolumes(context.Background(), "world")
	require.NoError(t, err)
	require.Equal(t, core.AccountWithVolumes{
		Account: core.Account{
			Address:  "world",
			Metadata: core.Metadata{},
		},
		Volumes: map[string]core.Volumes{},
	}, *worldAccount)

	require.NoError(t, ledgerStore.EnsureAccountExists(context.Background(), "bank"))
	bankAccount, err := cache.GetAccountWithVolumes(context.Background(), "bank")
	require.NoError(t, err)
	require.Equal(t, core.AccountWithVolumes{
		Account: core.Account{
			Address:  "bank",
			Metadata: core.Metadata{},
		},
		Volumes: map[string]core.Volumes{},
	}, *bankAccount)

	cache.UpdateVolumeWithTX(core.NewTransaction().WithPostings(
		core.NewPosting("world", "bank", "USD", core.NewMonetaryInt(100)),
	))
	worldAccount, err = cache.GetAccountWithVolumes(context.Background(), "world")
	require.NoError(t, err)
	require.EqualValues(t, core.AccountWithVolumes{
		Account: core.Account{
			Address:  "world",
			Metadata: core.Metadata{},
		},
		Volumes: map[string]core.Volumes{
			"USD": core.NewEmptyVolumes().WithOutput(core.NewMonetaryInt(100)),
		},
	}, *worldAccount)

	worldAccount, err = cache.GetAccountWithVolumes(context.Background(), "bank")
	require.NoError(t, err)
	require.Equal(t, core.AccountWithVolumes{
		Account: core.Account{
			Address:  "bank",
			Metadata: core.Metadata{},
		},
		Volumes: map[string]core.Volumes{
			"USD": core.NewEmptyVolumes().WithInput(core.NewMonetaryInt(100)),
		},
	}, *worldAccount)
}
