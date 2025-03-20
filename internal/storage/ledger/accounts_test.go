//go:build it

package ledger_test

import (
	"context"
	"github.com/formancehq/go-libs/v2/bun/bundebug"
	"math/big"
	"testing"
	libtime "time"

	"errors"
	"github.com/formancehq/go-libs/v2/pointer"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/logging"

	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
)

func TestAccountsList(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now().Add(-time.Minute)
	ctx := bundebug.WithDebug(logging.TestingContext())

	err := store.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
		WithTimestamp(now).
		WithInsertedAt(now)), nil)
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
		WithInsertedAt(now.Add(100*time.Millisecond))), nil)
	require.NoError(t, err)

	err = store.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().
		WithPostings(ledger.NewPosting("account:1", "bank", "USD", big.NewInt(50))).
		WithTimestamp(now.Add(3*time.Minute)).
		WithInsertedAt(now.Add(200*time.Millisecond))), nil)
	require.NoError(t, err)

	err = store.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(0))).
		WithTimestamp(now.Add(-time.Minute)).
		WithInsertedAt(now.Add(200*time.Millisecond))), nil)
	require.NoError(t, err)

	t.Run("list all", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 7)
	})

	t.Run("list using metadata", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Match("metadata[category]", "1"),
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1)
	})

	t.Run("list before date", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				PIT: &now,
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 2)
	})

	t.Run("list with volumes", func(t *testing.T) {
		t.Parallel()

		accounts, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Match("address", "account:1"),
				Expand:  []string{"volumes"},
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1)
		require.Equal(t, ledger.VolumesByAssets{
			"USD": ledger.NewVolumesInt64(200, 50),
		}, accounts.Data[0].Volumes)
	})

	t.Run("list with volumes using PIT", func(t *testing.T) {
		t.Parallel()

		accounts, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Match("address", "account:1"),
				PIT:     &now,
				Expand:  []string{"volumes"},
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1)
		require.Equal(t, ledger.VolumesByAssets{
			"USD": ledger.NewVolumesInt64(100, 0),
		}, accounts.Data[0].Volumes)
	})

	t.Run("list with effective volumes", func(t *testing.T) {
		t.Parallel()

		accounts, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Match("address", "account:1"),
				Expand:  []string{"effectiveVolumes"},
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1)
		require.Equal(t, ledger.VolumesByAssets{
			"USD": ledger.NewVolumesInt64(200, 50),
		}, accounts.Data[0].EffectiveVolumes)
	})

	t.Run("list with effective volumes using PIT", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Match("address", "account:1"),
				PIT:     &now,
				Expand:  []string{"effectiveVolumes"},
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1)
		require.Equal(t, ledger.VolumesByAssets{
			"USD": ledger.NewVolumesInt64(100, 0),
		}, accounts.Data[0].EffectiveVolumes)
	})

	t.Run("list using filter on address", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Match("address", "account:"),
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 3)
	})
	t.Run("list using filter on multiple address", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Or(
					query.Match("address", "account:1"),
					query.Match("address", "orders:"),
				),
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 3)
	})
	t.Run("list using filter on balances", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Lt("balance[USD]", 0),
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1) // world

		accounts, err = store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Gt("balance[USD]", 0),
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 2)
		require.Equal(t, "account:1", accounts.Data[0].Address)
		require.Equal(t, "bank", accounts.Data[1].Address)
	})
	t.Run("list using filter on balances[USD] and PIT", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Lt("balance[USD]", 0),
				PIT:     &now,
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1) // world
	})
	t.Run("list using filter on balances and PIT", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Lt("balance", 0),
				PIT:     &now,
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 1) // world
	})

	t.Run("list using filter on exists metadata", func(t *testing.T) {
		t.Parallel()
		accounts, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Exists("metadata", "foo"),
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 2)

		accounts, err = store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Exists("metadata", "category"),
			},
		})
		require.NoError(t, err)
		require.Len(t, accounts.Data, 3)
	})

	t.Run("list using filter invalid field", func(t *testing.T) {
		t.Parallel()
		_, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Lt("invalid", 0),
			},
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, ledgercontroller.ErrInvalidQuery{}))
	})

	t.Run("filter on first_usage", func(t *testing.T) {
		t.Parallel()

		ret, err := store.Accounts().Paginate(ctx, ledgercontroller.OffsetPaginatedQuery[any]{
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Lte("first_usage", now),
			},
		})
		require.NoError(t, err)
		require.Len(t, ret.Data, 2)
	})
}

func TestAccountsUpdateMetadata(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	m := metadata.Metadata{
		"foo": "bar",
	}
	ctx := logging.TestingContext()

	require.NoError(t, store.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
		"bank": m,
	}))

	account, err := store.Accounts().GetOne(context.Background(), ledgercontroller.ResourceQuery[any]{
		Builder: query.Match("address", "bank"),
	})
	require.NoError(t, err, "account retrieval should not fail")

	require.Equal(t, "bank", account.Address, "account address should match")
	require.Equal(t, m, account.Metadata, "account metadata should match")
}

func TestAccountsGet(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	tx1 := pointer.For(ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "multi", "USD/2", big.NewInt(100)),
	).WithTimestamp(now))
	err := store.CommitTransaction(ctx, tx1, nil)
	require.NoError(t, err)

	// sleep for at least the time precision to ensure the next transaction is inserted with a different timestamp
	libtime.Sleep(time.DatePrecision)

	require.NoError(t, store.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
		"multi": {
			"category": "gold",
		},
	}))

	tx2 := pointer.For(ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "multi", "USD/2", big.NewInt(0)),
	).WithTimestamp(now.Add(-time.Minute)))
	err = store.CommitTransaction(ctx, tx2, nil)
	require.NoError(t, err)

	t.Run("find account", func(t *testing.T) {
		t.Parallel()
		account, err := store.Accounts().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
			Builder: query.Match("address", "multi"),
		})
		require.NoError(t, err)
		require.Equal(t, ledger.Account{
			Address: "multi",
			Metadata: metadata.Metadata{
				"category": "gold",
			},
			FirstUsage:    now.Add(-time.Minute),
			InsertionDate: tx1.InsertedAt,
			UpdatedAt:     tx2.InsertedAt,
		}, *account)

		account, err = store.Accounts().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
			Builder: query.Match("address", "world"),
		})
		require.NoError(t, err)
		require.Equal(t, ledger.Account{
			Address:       "world",
			Metadata:      metadata.Metadata{},
			FirstUsage:    now.Add(-time.Minute),
			InsertionDate: tx1.InsertedAt,
			UpdatedAt:     tx2.InsertedAt,
		}, *account)
	})

	t.Run("find account in past", func(t *testing.T) {
		t.Parallel()

		account, err := store.Accounts().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
			Builder: query.Match("address", "multi"),
			PIT:     pointer.For(now.Add(-30 * time.Second)),
		})
		require.NoError(t, err)
		require.Equal(t, ledger.Account{
			Address:       "multi",
			Metadata:      metadata.Metadata{},
			FirstUsage:    now.Add(-time.Minute),
			InsertionDate: tx1.InsertedAt,
			UpdatedAt:     tx2.InsertedAt,
		}, *account)
	})

	t.Run("find account with volumes", func(t *testing.T) {
		t.Parallel()
		account, err := store.Accounts().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
			Builder: query.Match("address", "multi"),
			Expand:  []string{"volumes"},
		})
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
			InsertionDate: tx1.InsertedAt,
			UpdatedAt:     tx2.InsertedAt,
		}, *account)
	})

	t.Run("find account with effective volumes", func(t *testing.T) {
		t.Parallel()
		account, err := store.Accounts().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
			Builder: query.Match("address", "multi"),
			Expand:  []string{"effectiveVolumes"},
		})
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
			InsertionDate: tx1.InsertedAt,
			UpdatedAt:     tx2.InsertedAt,
		}, *account)
	})

	t.Run("find account using pit", func(t *testing.T) {
		t.Parallel()

		account, err := store.Accounts().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
			Builder: query.Match("address", "multi"),
			PIT:     pointer.For(now),
		})
		require.NoError(t, err)
		require.Equal(t, ledger.Account{
			Address:       "multi",
			Metadata:      metadata.Metadata{},
			FirstUsage:    now.Add(-time.Minute),
			InsertionDate: tx1.InsertedAt,
			UpdatedAt:     tx2.InsertedAt,
		}, *account)
	})

	t.Run("not existent account", func(t *testing.T) {
		t.Parallel()

		_, err := store.Accounts().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
			Builder: query.Match("address", "account_not_existing"),
		})
		require.Error(t, err)
	})
}

func TestAccountsCount(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	err := store.CommitTransaction(ctx, pointer.For(ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "central_bank", "USD/2", big.NewInt(100)),
	)), nil)
	require.NoError(t, err)

	countAccounts, err := store.Accounts().Count(ctx, ledgercontroller.ResourceQuery[any]{})
	require.NoError(t, err)
	require.EqualValues(t, 2, countAccounts) // world + central_bank
}

func TestAccountsUpsert(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	account1 := ledger.Account{
		Address: "foo",
	}

	account2 := ledger.Account{
		Address: "foo2",
	}

	// Initial insert
	err := store.UpsertAccounts(ctx, &account1, &account2)
	require.NoError(t, err)

	require.NotEmpty(t, account1.FirstUsage)
	require.NotEmpty(t, account1.InsertionDate)
	require.NotEmpty(t, account1.UpdatedAt)

	require.NotEmpty(t, account2.FirstUsage)
	require.NotEmpty(t, account2.InsertionDate)
	require.NotEmpty(t, account2.UpdatedAt)

	now := time.Now()

	// Reset the account model
	account1 = ledger.Account{
		Address: "foo",
		// The account will be upserted on the timeline after its initial usage.
		// The upsert should not modify anything, but, it should retrieve and load the account entity
		FirstUsage:    now.Add(time.Second),
		InsertionDate: now.Add(time.Second),
		UpdatedAt:     now.Add(time.Second),
	}

	// Upsert with no modification
	err = store.UpsertAccounts(ctx, &account1)
	require.NoError(t, err)
}
