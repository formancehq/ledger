//go:build it

package ledger

import (
	"context"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/pointer"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/logging"

	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
)

func TestGetAccounts(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	err := store.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
		WithTimestamp(now).
		WithInsertedAt(now)))
	require.NoError(t, err)

	require.NoError(t, store.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
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

	err = store.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
		WithTimestamp(now.Add(4*time.Minute)).
		WithInsertedAt(now.Add(100*time.Millisecond))))
	require.NoError(t, err)

	err = store.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().
		WithPostings(ledger.NewPosting("account:1", "bank", "USD", big.NewInt(50))).
		WithTimestamp(now.Add(3*time.Minute)).
		WithInsertedAt(now.Add(200*time.Millisecond))))
	require.NoError(t, err)

	err = store.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(0))).
		WithTimestamp(now.Add(-time.Minute)).
		WithInsertedAt(now.Add(200*time.Millisecond))))
	require.NoError(t, err)

	t.Run("list all", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{})))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 7)
	})

	t.Run("list using metadata", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Match("metadata[category]", "1")),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1)
	})

	t.Run("list before date", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
			PITFilter: ledgercontroller.PITFilter{
				PIT: &now,
			},
		})))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 2)
	})

	t.Run("list with volumes", func(t *testing.T) {
		t.Parallel()

		accounts, err := store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
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

		accounts, err := store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
			PITFilter: ledgercontroller.PITFilter{
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

		accounts, err := store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
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
		accounts, err := store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
			PITFilter: ledgercontroller.PITFilter{
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
		accounts, err := store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Match("address", "account:")),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 3)
	})
	t.Run("list using filter on multiple address", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
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
		accounts, err := store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Lt("balance[USD]", 0)),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1) // world

		accounts, err = store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Gt("balance[USD]", 0)),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 2)
		require.Equal(t, "account:1", accounts.Data[0].Address)
		require.Equal(t, "bank", accounts.Data[1].Address)
	})

	t.Run("list using filter on exists metadata", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Exists("metadata", "foo")),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 2)

		accounts, err = store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Exists("metadata", "category")),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 3)
	})

	t.Run("list using filter invalid field", func(t *testing.T) {
		t.Parallel()
		_, err := store.ListAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
			WithQueryBuilder(query.Lt("invalid", 0)),
		))
		require.Error(t, err)
		require.True(t, errors.Is(err, ledgercontroller.ErrInvalidQuery{}))
	})
}

func TestUpdateAccountsMetadata(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	m := metadata.Metadata{
		"foo": "bar",
	}

	ctx := logging.TestingContext()

	require.NoError(t, store.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
		"bank": m,
	}))

	account, err := store.GetAccount(context.Background(), ledgercontroller.NewGetAccountQuery("bank"))
	require.NoError(t, err, "account retrieval should not fail")

	require.Equal(t, "bank", account.Address, "account address should match")
	require.Equal(t, m, account.Metadata, "account metadata should match")
}

func TestGetAccount(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	err := store.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "multi", "USD/2", big.NewInt(100)),
	).WithTimestamp(now)))
	require.NoError(t, err)

	require.NoError(t, store.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
		"multi": {
			"category": "gold",
		},
	}))

	err = store.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "multi", "USD/2", big.NewInt(0)),
	).WithTimestamp(now.Add(-time.Minute))))
	require.NoError(t, err)

	t.Run("find account", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccount(ctx, ledgercontroller.NewGetAccountQuery("multi"))
		require.NoError(t, err)
		require.Equal(t, ledger.Account{
			Address: "multi",
			Metadata: metadata.Metadata{
				"category": "gold",
			},
			FirstUsage:       now.Add(-time.Minute),
			EffectiveVolumes: map[string]ledger.Volumes{},
		}, *account)

		account, err = store.GetAccount(ctx, ledgercontroller.NewGetAccountQuery("world"))
		require.NoError(t, err)
		require.Equal(t, ledger.Account{
			Address:          "world",
			Metadata:         metadata.Metadata{},
			FirstUsage:       now.Add(-time.Minute),
			EffectiveVolumes: ledger.VolumesByAssets{},
		}, *account)
	})

	t.Run("find account in past", func(t *testing.T) {
		t.Parallel()

		account, err := store.GetAccount(ctx, ledgercontroller.NewGetAccountQuery("multi").WithPIT(now.Add(-30*time.Second)))
		require.NoError(t, err)
		require.Equal(t, ledger.Account{
			Address:          "multi",
			Metadata:         metadata.Metadata{},
			FirstUsage:       now.Add(-time.Minute),
			EffectiveVolumes: ledger.VolumesByAssets{},
		}, *account)
	})

	t.Run("find account with volumes", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccount(ctx, ledgercontroller.NewGetAccountQuery("multi").
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
			EffectiveVolumes: map[string]ledger.Volumes{},
		}, *account)
	})

	t.Run("find account with effective volumes", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccount(ctx, ledgercontroller.NewGetAccountQuery("multi").
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

	// todo: sometimes failing, need to debug
	//t.Run("find account using pit", func(t *testing.T) {
	//	t.Parallel()
	//
	//	account, err := store.GetAccount(ctx, ledgercontroller.NewGetAccountQuery("multi").WithPIT(now))
	//	require.NoError(t, err)
	//	require.Equal(t, ledger.Account{
	//		Account: ledger.Account{
	//			Address:    "multi",
	//			Metadata:   metadata.Metadata{},
	//			FirstUsage: now.Add(-time.Minute),
	//		},
	//		AggregatedAccountVolumes:          map[string]ledger.AggregatedAccountVolumes{},
	//		EffectiveVolumes: map[string]ledger.AggregatedAccountVolumes{},
	//	}, *account)
	//})

	t.Run("not existent account", func(t *testing.T) {
		t.Parallel()
		_, err := store.GetAccount(ctx, ledgercontroller.NewGetAccountQuery("account_not_existing"))
		require.Error(t, err)
	})

}

func TestGetAccountWithVolumes(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := logging.TestingContext()
	now := time.Now()

	bigInt, _ := big.NewInt(0).SetString("999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", 10)

	err := store.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "multi", "USD/2", bigInt),
	).WithTimestamp(now)))
	require.NoError(t, err)

	accountWithVolumes, err := store.GetAccount(ctx,
		ledgercontroller.NewGetAccountQuery("multi").WithExpandVolumes())
	require.NoError(t, err)
	require.Equal(t, &ledger.Account{
		Address:    "multi",
		Metadata:   metadata.Metadata{},
		FirstUsage: now,
		Volumes: map[string]ledger.Volumes{
			"USD/2": ledger.NewEmptyVolumes().WithInput(bigInt),
		},
		EffectiveVolumes: map[string]ledger.Volumes{},
	}, accountWithVolumes)
}

func TestUpdateAccountMetadata(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	require.NoError(t, store.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
		"central_bank": {
			"foo": "bar",
		},
	}))

	account, err := store.GetAccount(ctx, ledgercontroller.NewGetAccountQuery("central_bank"))
	require.NoError(t, err)
	require.EqualValues(t, "bar", account.Metadata["foo"])
}

func TestCountAccounts(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	err := store.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "central_bank", "USD/2", big.NewInt(100)),
	)))
	require.NoError(t, err)

	countAccounts, err := store.CountAccounts(ctx, ledgercontroller.NewListAccountsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{})))
	require.NoError(t, err)
	require.EqualValues(t, 2, countAccounts) // world + central_bank
}

func TestUpsertAccount(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	now := time.Now()

	account := Account{
		Ledger:        store.Name(),
		Address:       "foo",
		FirstUsage:    now,
		InsertionDate: now,
		UpdatedAt:     now,
	}

	// initial insert
	upserted, err := store.upsertAccount(ctx, &account)
	require.NoError(t, err)
	require.True(t, upserted)

	// reset the account model
	account = Account{
		Ledger:  store.Name(),
		Address: "foo",
		// the account will be upserted on the timeline after its initial usage
		// the upsert should not modify anything
		// but, it should retrieve and load the account entity
		FirstUsage:    now.Add(time.Second),
		InsertionDate: now.Add(time.Second),
		UpdatedAt:     now.Add(time.Second),
	}

	// upsert with no modification
	upserted, err = store.upsertAccount(ctx, &account)
	require.NoError(t, err)
	require.False(t, upserted)
}
