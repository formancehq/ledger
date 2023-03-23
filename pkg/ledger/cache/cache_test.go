package cache

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type mockCall struct {
	address string
}

type mockAccountComputer struct {
	accounts []*core.AccountWithVolumes
	calls    []mockCall
}

func (c *mockAccountComputer) ComputeAccount(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	c.calls = append(c.calls, mockCall{
		address: address,
	})
	for _, accountWithVolumes := range c.accounts {
		if accountWithVolumes.Address == address {
			return accountWithVolumes, nil
		}
	}
	return nil, nil
}

func TestParallelRead(t *testing.T) {

	mock := &mockAccountComputer{
		accounts: []*core.AccountWithVolumes{
			{
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
			},
			{
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
			},
		},
	}
	cache := New(mock)

	eg := errgroup.Group{}
	for i := 0; i < 1000; i++ {
		eg.Go(func() error {
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
			return nil
		})
	}

	require.NoError(t, eg.Wait())
	require.Len(t, mock.calls, 1)
}

func TestUpdateVolumes(t *testing.T) {

	mock := &mockAccountComputer{
		accounts: []*core.AccountWithVolumes{
			{
				Account: core.Account{
					Address: "world",
				},
				Volumes: map[string]core.Volumes{},
			},
			{
				Account: core.Account{
					Address: "bank",
				},
				Volumes: map[string]core.Volumes{},
			},
		},
	}
	cache := New(mock)

	// Force load accounts
	_, err := cache.GetAccountWithVolumes(context.Background(), "world")
	require.NoError(t, err)

	_, err = cache.GetAccountWithVolumes(context.Background(), "bank")
	require.NoError(t, err)

	cache.UpdateVolumeWithTX(core.NewTransaction().WithPostings(
		core.NewPosting("world", "bank", "USD", core.NewMonetaryInt(100)),
	))
	worldAccount, err := cache.GetAccountWithVolumes(context.Background(), "world")
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