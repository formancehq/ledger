//go:build it

package legacy_test

import (
	"github.com/formancehq/go-libs/v2/pointer"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/ledger/legacy"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/logging"

	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger/legacy"
	"github.com/stretchr/testify/require"
)

func TestGetAccounts(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	err := store.newStore.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
		WithTimestamp(now).
		WithInsertedAt(now)), nil)
	require.NoError(t, err)

	require.NoError(t, store.newStore.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
		"account:1": {
			"category": "1",
		},
		"account:2": {
			"category": "2",
		},
		"account:3": {
			"category": "3",
		},
		"orders:1": {
			"foo": "bar",
		},
		"orders:2": {
			"foo": "bar",
		},
	}))

	err = store.newStore.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
		WithTimestamp(now.Add(4*time.Minute)).
		WithInsertedAt(now.Add(100*time.Millisecond))), nil)
	require.NoError(t, err)

	err = store.newStore.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().
		WithPostings(ledger.NewPosting("account:1", "bank", "USD", big.NewInt(50))).
		WithTimestamp(now.Add(3*time.Minute)).
		WithInsertedAt(now.Add(200*time.Millisecond))), nil)
	require.NoError(t, err)

	err = store.newStore.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(0))).
		WithTimestamp(now.Add(-time.Minute)).
		WithInsertedAt(now.Add(200*time.Millisecond))), nil)
	require.NoError(t, err)

	t.Run("list all", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{})))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 7)
	})

	t.Run("list using metadata", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Match("metadata[category]", "1")),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1)
	})

	t.Run("list before date", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
			PITFilter: ledgerstore.PITFilter{
				PIT: &now,
			},
		})))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 2)
	})

	t.Run("list with volumes", func(t *testing.T) {
		t.Parallel()

		accounts, err := store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
			ExpandVolumes: true,
		}).WithQueryBuilder(query.Match("address", "account:1"))))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1)
		require.Equal(t, ledger.VolumesByAssets{
			"USD": ledger.NewVolumesInt64(200, 50),
		}, accounts.Data[0].Volumes)
	})

	t.Run("list with volumes using PIT", func(t *testing.T) {
		t.Parallel()

		accounts, err := store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
			PITFilter: ledgerstore.PITFilter{
				PIT: &now,
			},
			ExpandVolumes: true,
		}).WithQueryBuilder(query.Match("address", "account:1"))))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1)
		require.Equal(t, ledger.VolumesByAssets{
			"USD": ledger.NewVolumesInt64(100, 0),
		}, accounts.Data[0].Volumes)
	})

	t.Run("list with effective volumes", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
			ExpandEffectiveVolumes: true,
		}).WithQueryBuilder(query.Match("address", "account:1"))))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1)
		require.Equal(t, ledger.VolumesByAssets{
			"USD": ledger.NewVolumesInt64(200, 50),
		}, accounts.Data[0].EffectiveVolumes)
	})

	t.Run("list with effective volumes using PIT", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
			PITFilter: ledgerstore.PITFilter{
				PIT: &now,
			},
			ExpandEffectiveVolumes: true,
		}).WithQueryBuilder(query.Match("address", "account:1"))))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1)
		require.Equal(t, ledger.VolumesByAssets{
			"USD": ledger.NewVolumesInt64(100, 0),
		}, accounts.Data[0].EffectiveVolumes)
	})

	t.Run("list using filter on address", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Match("address", "account:")),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 3)
	})
	t.Run("list using filter on multiple address", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
			WithQueryBuilder(
				query.Or(
					query.Match("address", "account:1"),
					query.Match("address", "orders:"),
				),
			),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 3)
	})
	t.Run("list using filter on balances", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Lt("balance[USD]", 0)),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1) // world

		accounts, err = store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Gt("balance[USD]", 0)),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 2)
		require.Equal(t, "account:1", accounts.Data[0].Address)
		require.Equal(t, "bank", accounts.Data[1].Address)
	})

	t.Run("list using filter on exists metadata", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Exists("metadata", "foo")),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 2)

		accounts, err = store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Exists("metadata", "category")),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 3)
	})

	t.Run("list using filter invalid field", func(t *testing.T) {
		t.Parallel()
		_, err := store.GetAccountsWithVolumes(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Lt("invalid", 0)),
		))
		require.Error(t, err)
		require.True(t, legacy.IsErrInvalidQuery(err))
	})
}

func TestGetAccount(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	err := store.newStore.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "multi", "USD/2", big.NewInt(100)),
	).WithTimestamp(now)), nil)
	require.NoError(t, err)

	require.NoError(t, store.newStore.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
		"multi": {
			"category": "gold",
		},
	}))

	err = store.newStore.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "multi", "USD/2", big.NewInt(0)),
	).WithTimestamp(now.Add(-time.Minute))), nil)
	require.NoError(t, err)

	t.Run("find account", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccountWithVolumes(ctx, ledgerstore.NewGetAccountQuery("multi"))
		require.NoError(t, err)
		require.Equal(t, ledger.Account{
			Address: "multi",
			Metadata: metadata.Metadata{
				"category": "gold",
			},
			FirstUsage: now.Add(-time.Minute),
		}, *account)

		account, err = store.GetAccountWithVolumes(ctx, ledgerstore.NewGetAccountQuery("world"))
		require.NoError(t, err)
		require.Equal(t, ledger.Account{
			Address:    "world",
			Metadata:   metadata.Metadata{},
			FirstUsage: now.Add(-time.Minute),
		}, *account)
	})

	t.Run("find account in past", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccountWithVolumes(ctx, ledgerstore.NewGetAccountQuery("multi").WithPIT(now.Add(-30*time.Second)))
		require.NoError(t, err)
		require.Equal(t, ledger.Account{
			Address:    "multi",
			Metadata:   metadata.Metadata{},
			FirstUsage: now.Add(-time.Minute),
		}, *account)
	})

	t.Run("find account with volumes", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccountWithVolumes(ctx, ledgerstore.NewGetAccountQuery("multi").
			WithExpandVolumes())
		require.NoError(t, err)
		require.Equal(t, ledger.Account{
			Address: "multi",
			Metadata: metadata.Metadata{
				"category": "gold",
			},
			FirstUsage: now.Add(-time.Minute),
			Volumes: ledger.VolumesByAssets{
				"USD/2": ledger.NewVolumesInt64(100, 0),
			},
		}, *account)
	})

	t.Run("find account with effective volumes", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccountWithVolumes(ctx, ledgerstore.NewGetAccountQuery("multi").
			WithExpandEffectiveVolumes())
		require.NoError(t, err)
		require.Equal(t, ledger.Account{
			Address: "multi",
			Metadata: metadata.Metadata{
				"category": "gold",
			},
			FirstUsage: now.Add(-time.Minute),

			EffectiveVolumes: ledger.VolumesByAssets{
				"USD/2": ledger.NewVolumesInt64(100, 0),
			},
		}, *account)
	})

	t.Run("find account using pit", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccountWithVolumes(ctx, ledgerstore.NewGetAccountQuery("multi").WithPIT(now))
		require.NoError(t, err)
		require.Equal(t, ledger.Account{
			Address:    "multi",
			Metadata:   metadata.Metadata{},
			FirstUsage: now.Add(-time.Minute),
		}, *account)
	})

	t.Run("not existent account", func(t *testing.T) {
		t.Parallel()
		_, err := store.GetAccountWithVolumes(ctx, ledgerstore.NewGetAccountQuery("account_not_existing"))
		require.Error(t, err)
	})

}

func TestCountAccounts(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	err := store.newStore.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "central_bank", "USD/2", big.NewInt(100)),
	)), nil)
	require.NoError(t, err)

	countAccounts, err := store.CountAccounts(ctx, ledgerstore.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{})))
	require.NoError(t, err)
	require.EqualValues(t, 2, countAccounts) // world + central_bank
}
