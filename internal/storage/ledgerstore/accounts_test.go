package ledgerstore

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/stack/libs/go-libs/logging"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/stretchr/testify/require"
)

func TestGetAccounts(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()
	ctx := logging.TestingContext()

	require.NoError(t, store.InsertLogs(ctx,
		ledger.ChainLogs(
			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
					WithDate(now),
				map[string]metadata.Metadata{
					"account:1": {
						"category": "4",
					},
				},
			).WithDate(now),
			ledger.NewSetMetadataOnAccountLog(ledger.Now(), "account:1", metadata.Metadata{"category": "1"}).WithDate(now.Add(time.Minute)),
			ledger.NewSetMetadataOnAccountLog(ledger.Now(), "account:2", metadata.Metadata{"category": "2"}).WithDate(now.Add(2*time.Minute)),
			ledger.NewSetMetadataOnAccountLog(ledger.Now(), "account:3", metadata.Metadata{"category": "3"}).WithDate(now.Add(3*time.Minute)),
			ledger.NewSetMetadataOnAccountLog(ledger.Now(), "orders:1", metadata.Metadata{"foo": "bar"}).WithDate(now.Add(3*time.Minute)),
			ledger.NewSetMetadataOnAccountLog(ledger.Now(), "orders:2", metadata.Metadata{"foo": "bar"}).WithDate(now.Add(3*time.Minute)),
			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
					WithIDUint64(1).
					WithDate(now.Add(4*time.Minute)),
				map[string]metadata.Metadata{},
			).WithDate(now.Add(100*time.Millisecond)),
			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("account:1", "bank", "USD", big.NewInt(50))).
					WithDate(now.Add(3*time.Minute)).
					WithIDUint64(2),
				map[string]metadata.Metadata{},
			).WithDate(now.Add(200*time.Millisecond)),
		)...,
	))

	t.Run("list all", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, NewGetAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{})))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 7)
	})

	t.Run("list using metadata", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, NewGetAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{}).
			WithQueryBuilder(query.Match("metadata[category]", "1")),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1)
	})

	t.Run("list before date", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, NewGetAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{
			PITFilter: PITFilter{
				PIT: &now,
			},
		})))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 2)
	})

	t.Run("list with volumes", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, NewGetAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{
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
		accounts, err := store.GetAccountsWithVolumes(ctx, NewGetAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{
			PITFilter: PITFilter{
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
		accounts, err := store.GetAccountsWithVolumes(ctx, NewGetAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{
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
		accounts, err := store.GetAccountsWithVolumes(ctx, NewGetAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{
			PITFilter: PITFilter{
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
		accounts, err := store.GetAccountsWithVolumes(ctx, NewGetAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{}).
			WithQueryBuilder(query.Match("address", "account:")),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 3)
	})
	t.Run("list using filter on multiple address", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.GetAccountsWithVolumes(ctx, NewGetAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{}).
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
		accounts, err := store.GetAccountsWithVolumes(ctx, NewGetAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{}).
			WithQueryBuilder(query.Lt("balance[USD]", 0)),
		))
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1) // world
	})
	t.Run("list using filter invalid field", func(t *testing.T) {
		t.Parallel()
		_, err := store.GetAccountsWithVolumes(ctx, NewGetAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{}).
			WithQueryBuilder(query.Lt("invalid", 0)),
		))
		require.Error(t, err)
		require.True(t, IsErrInvalidQuery(err))
	})
}

func TestUpdateAccountsMetadata(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	metadata := metadata.Metadata{
		"foo": "bar",
	}

	require.NoError(t, store.InsertLogs(context.Background(),
		ledger.NewSetMetadataOnAccountLog(ledger.Now(), "bank", metadata).ChainLog(nil),
	), "account insertion should not fail")

	account, err := store.GetAccountWithVolumes(context.Background(), NewGetAccountQuery("bank"))
	require.NoError(t, err, "account retrieval should not fail")

	require.Equal(t, "bank", account.Address, "account address should match")
	require.Equal(t, metadata, account.Metadata, "account metadata should match")
}

func TestGetAccount(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()
	ctx := logging.TestingContext()

	require.NoError(t, store.InsertLogs(ctx,
		ledger.ChainLogs(
			ledger.NewTransactionLog(ledger.NewTransaction().WithPostings(
				ledger.NewPosting("world", "multi", "USD/2", big.NewInt(100)),
			).WithDate(now), map[string]metadata.Metadata{}),
			ledger.NewSetMetadataLog(now.Add(time.Minute), ledger.SetMetadataLogPayload{
				TargetType: ledger.MetaTargetTypeAccount,
				TargetID:   "multi",
				Metadata: metadata.Metadata{
					"category": "gold",
				},
			}),
		)...,
	))

	t.Run("find account", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccountWithVolumes(ctx, NewGetAccountQuery("multi"))
		require.NoError(t, err)
		require.Equal(t, ledger.ExpandedAccount{
			Account: ledger.Account{
				Address: "multi",
				Metadata: metadata.Metadata{
					"category": "gold",
				},
			},
		}, *account)

		account, err = store.GetAccountWithVolumes(ctx, NewGetAccountQuery("world"))
		require.NoError(t, err)
		require.Equal(t, ledger.ExpandedAccount{
			Account: ledger.Account{
				Address:  "world",
				Metadata: metadata.Metadata{},
			},
		}, *account)
	})

	t.Run("find account in past", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccountWithVolumes(ctx, NewGetAccountQuery("multi"))
		require.NoError(t, err)
		require.Equal(t, ledger.ExpandedAccount{
			Account: ledger.Account{
				Address: "multi",
				Metadata: metadata.Metadata{
					"category": "gold",
				},
			},
		}, *account)

		account, err = store.GetAccountWithVolumes(ctx, NewGetAccountQuery("world").WithPIT(ledger.Now()))
		require.NoError(t, err)
		require.Equal(t, ledger.ExpandedAccount{
			Account: ledger.Account{
				Address:  "world",
				Metadata: metadata.Metadata{},
			},
		}, *account)
	})

	t.Run("find account with volumes", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccountWithVolumes(ctx, NewGetAccountQuery("multi").
			WithExpandVolumes())
		require.NoError(t, err)
		require.Equal(t, ledger.ExpandedAccount{
			Account: ledger.Account{
				Address: "multi",
				Metadata: metadata.Metadata{
					"category": "gold",
				},
			},
			Volumes: ledger.VolumesByAssets{
				"USD/2": ledger.NewVolumesInt64(100, 0),
			},
		}, *account)
	})

	t.Run("find account with effective volumes", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccountWithVolumes(ctx, NewGetAccountQuery("multi").
			WithExpandEffectiveVolumes())
		require.NoError(t, err)
		require.Equal(t, ledger.ExpandedAccount{
			Account: ledger.Account{
				Address: "multi",
				Metadata: metadata.Metadata{
					"category": "gold",
				},
			},
			EffectiveVolumes: ledger.VolumesByAssets{
				"USD/2": ledger.NewVolumesInt64(100, 0),
			},
		}, *account)
	})

	t.Run("find account using pit", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccountWithVolumes(ctx, NewGetAccountQuery("multi").WithPIT(now))
		require.NoError(t, err)
		require.Equal(t, ledger.ExpandedAccount{
			Account: ledger.Account{
				Address:  "multi",
				Metadata: metadata.Metadata{},
			},
			Volumes: ledger.VolumesByAssets{},
		}, *account)
	})

	t.Run("not existent account", func(t *testing.T) {
		t.Parallel()
		account, err := store.GetAccountWithVolumes(ctx, NewGetAccountQuery("account_not_existing"))
		require.NoError(t, err)
		require.NotNil(t, account)
	})
}

func TestGetAccountWithVolumes(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	bigInt, _ := big.NewInt(0).SetString("999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", 10)

	require.NoError(t, insertTransactions(context.Background(), store,
		*ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", "multi", "USD/2", bigInt),
		),
	))

	accountWithVolumes, err := store.GetAccountWithVolumes(context.Background(),
		NewGetAccountQuery("multi").WithExpandVolumes())
	require.NoError(t, err)
	require.Equal(t, &ledger.ExpandedAccount{
		Account: ledger.Account{
			Address:  "multi",
			Metadata: metadata.Metadata{},
		},
		Volumes: map[string]*ledger.Volumes{
			"USD/2": ledger.NewEmptyVolumes().WithInput(bigInt),
		},
	}, accountWithVolumes)
}

func TestUpdateAccountMetadata(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	require.NoError(t, store.InsertLogs(context.Background(),
		ledger.NewSetMetadataOnAccountLog(ledger.Now(), "central_bank", metadata.Metadata{
			"foo": "bar",
		}).ChainLog(nil),
	))

	account, err := store.GetAccountWithVolumes(context.Background(), NewGetAccountQuery("central_bank"))
	require.NoError(t, err)
	require.EqualValues(t, "bar", account.Metadata["foo"])
}

func TestCountAccounts(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	require.NoError(t, insertTransactions(context.Background(), store,
		*ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", "central_bank", "USD/2", big.NewInt(100)),
		),
	))

	countAccounts, err := store.CountAccounts(context.Background(), NewGetAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{})))
	require.NoError(t, err)
	require.EqualValues(t, 2, countAccounts) // world + central_bank
}
