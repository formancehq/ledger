package cache

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/alitto/pond"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type mockAccountComputer struct {
	accounts []*core.AccountWithVolumes
	calls    []string
}

func (c *mockAccountComputer) GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	c.calls = append(c.calls, address)
	for _, accountWithVolumes := range c.accounts {
		if accountWithVolumes.Address == address {
			return accountWithVolumes, nil
		}
	}
	return nil, storage.ErrNotFound
}

func TestParallelRead(t *testing.T) {

	mock := &mockAccountComputer{
		accounts: []*core.AccountWithVolumes{
			{
				Account: core.Account{
					Address:  "world",
					Metadata: metadata.Metadata{},
				},
				Volumes: map[string]core.Volumes{
					"USD/2": {
						Input:  big.NewInt(100),
						Output: big.NewInt(0),
					},
				},
			},
			{
				Account: core.Account{
					Address: "bank",
					Metadata: metadata.Metadata{
						"category": "gold",
					},
				},
				Volumes: map[string]core.Volumes{
					"USD/2": {
						Input:  big.NewInt(10),
						Output: big.NewInt(0),
					},
				},
			},
		},
	}
	cache := New(mock)
	go func() {
		require.NoError(t, cache.Run(context.Background()))
	}()
	defer func() {
		require.NoError(t, cache.Stop(context.Background()))
	}()

	release, err := cache.LockAccounts(context.Background(), "bank")
	require.NoError(t, err)
	defer func() {
		release()
	}()

	eg := errgroup.Group{}
	for i := 0; i < 1000; i++ {
		eg.Go(func() error {
			account, err := cache.GetAccountWithVolumes(context.Background(), "bank")
			require.NoError(t, err)
			require.NotNil(t, account)
			require.Equal(t, core.AccountWithVolumes{
				Account: core.Account{
					Address: "bank",
					Metadata: metadata.Metadata{
						"category": "gold",
					},
				},
				Volumes: map[string]core.Volumes{
					"USD/2": {
						Input:  big.NewInt(10),
						Output: big.NewInt(0),
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
	go func() {
		require.NoError(t, cache.Run(context.Background()))
	}()
	defer func() {
		require.NoError(t, cache.Stop(context.Background()))
	}()

	release, err := cache.LockAccounts(context.Background(), "world", "bank")
	require.NoError(t, err)
	defer func() {
		release()
	}()

	// Force load accounts
	_, err = cache.GetAccountWithVolumes(context.Background(), "world")
	require.NoError(t, err)

	_, err = cache.GetAccountWithVolumes(context.Background(), "bank")
	require.NoError(t, err)

	cache.UpdateVolumeWithTX(core.NewTransaction().WithPostings(
		core.NewPosting("world", "bank", "USD", big.NewInt(100)),
	))
	worldAccount, err := cache.GetAccountWithVolumes(context.Background(), "world")
	require.NoError(t, err)
	require.EqualValues(t, core.AccountWithVolumes{
		Account: core.Account{
			Address:  "world",
			Metadata: metadata.Metadata{},
		},
		Volumes: map[string]core.Volumes{
			"USD": core.NewEmptyVolumes().WithOutput(big.NewInt(100)),
		},
	}, *worldAccount)

	worldAccount, err = cache.GetAccountWithVolumes(context.Background(), "bank")
	require.NoError(t, err)
	require.Equal(t, core.AccountWithVolumes{
		Account: core.Account{
			Address:  "bank",
			Metadata: metadata.Metadata{},
		},
		Volumes: map[string]core.Volumes{
			"USD": core.NewEmptyVolumes().WithInput(big.NewInt(100)),
		},
	}, *worldAccount)
}

func TestCacheEviction(t *testing.T) {

	evictionPeriod := 100 * time.Millisecond
	mock := &mockAccountComputer{
		accounts: []*core.AccountWithVolumes{
			{
				Account: core.Account{
					Address: "world",
				},
				Volumes: map[string]core.Volumes{},
			},
		},
	}
	cache := New(mock, WithEvictionPeriod(evictionPeriod), WithRetainDelay(evictionPeriod))
	go func() {
		require.NoError(t, cache.Run(context.Background()))
	}()
	defer func() {
		require.NoError(t, cache.Stop(context.Background()))
	}()

	_, err := cache.GetAccountWithVolumes(context.Background(), "world")
	require.NoError(t, err)
	require.Equal(t, 1, cache.Count())

	require.Eventually(t, func() bool {
		return cache.Count() == 0
	}, 3*evictionPeriod, evictionPeriod)

	_, err = cache.LockAccounts(context.Background(), "world")
	require.NoError(t, err)

	<-time.After(2 * evictionPeriod)

	require.Equal(t, 1, cache.Count())
}

func BenchmarkCache(b *testing.B) {
	accounts := []*core.AccountWithVolumes{
		{
			Account: core.Account{
				Address: "world",
			},
			Volumes: map[string]core.Volumes{},
		},
	}
	accountsNumber := 100
	for i := 0; i < accountsNumber; i++ {
		accounts = append(accounts, &core.AccountWithVolumes{
			Account: core.NewAccount(fmt.Sprintf("account:%d", i)),
			Volumes: map[string]core.Volumes{},
		})
	}
	mock := &mockAccountComputer{
		accounts: accounts,
	}
	cache := New(mock)
	go func() {
		require.NoError(b, cache.Run(context.Background()))
	}()
	defer func() {
		require.NoError(b, cache.Stop(context.Background()))
	}()

	pool := pond.New(accountsNumber, accountsNumber)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.Submit(func() {
			_, err := cache.GetAccountWithVolumes(context.Background(), fmt.Sprintf("account:%d", i%accountsNumber))
			require.NoError(b, err)
		})
	}
	pool.StopAndWait()
}
