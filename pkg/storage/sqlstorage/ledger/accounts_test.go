package ledger_test

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

func TestAccounts(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	t.Run("success balance", func(t *testing.T) {
		q := storage.NewAccountsQuery().
			WithPageSize(10).
			WithBalanceFilter("50")

		_, err := store.GetAccounts(context.Background(), q)
		require.NoError(t, err, "balance filter should not fail")
	})

	t.Run("panic invalid balance", func(t *testing.T) {
		q := storage.NewAccountsQuery().
			WithPageSize(10).
			WithBalanceFilter("TEST")

		require.PanicsWithError(
			t, `invalid balance parameter: strconv.ParseInt: parsing "TEST": invalid syntax`,

			func() {
				_, _ = store.GetAccounts(context.Background(), q)
			}, "invalid balance in storage should panic")
	})

	t.Run("panic invalid balance operator", func(t *testing.T) {
		require.PanicsWithValue(t, "invalid balance operator parameter", func() {
			q := storage.NewAccountsQuery().
				WithPageSize(10).
				WithBalanceFilter("50").
				WithBalanceOperatorFilter("TEST")

			_, _ = store.GetAccounts(context.Background(), q)
		}, "invalid balance operator in storage should panic")
	})

	t.Run("success balance operator", func(t *testing.T) {
		q := storage.NewAccountsQuery().
			WithPageSize(10).
			WithBalanceFilter("50").
			WithBalanceOperatorFilter(storage.BalanceOperatorLte)

		_, err := store.GetAccounts(context.Background(), q)
		require.NoError(t, err, "balance operator filter should not fail")
	})

	t.Run("success account insertion", func(t *testing.T) {
		addr := "test:account"
		metadata := metadata.Metadata{
			"foo": "bar",
		}

		err := store.UpdateAccountMetadata(context.Background(), addr, metadata)
		require.NoError(t, err, "account insertion should not fail")

		account, err := store.GetAccount(context.Background(), addr)
		require.NoError(t, err, "account retrieval should not fail")

		require.Equal(t, addr, account.Address, "account address should match")
		require.Equal(t, metadata, account.Metadata, "account metadata should match")
	})

	t.Run("success multiple account insertions", func(t *testing.T) {
		accounts := []core.Account{
			{
				Address:  "test:account1",
				Metadata: metadata.Metadata{"foo1": "bar1"},
			},
			{
				Address:  "test:account2",
				Metadata: metadata.Metadata{"foo2": "bar2"},
			},
			{
				Address:  "test:account3",
				Metadata: metadata.Metadata{"foo3": "bar3"},
			},
		}

		err := store.UpdateAccountsMetadata(context.Background(), accounts)
		require.NoError(t, err, "account insertion should not fail")

		for _, account := range accounts {
			acc, err := store.GetAccount(context.Background(), account.Address)
			require.NoError(t, err, "account retrieval should not fail")

			require.Equal(t, account.Address, acc.Address, "account address should match")
			require.Equal(t, account.Metadata, acc.Metadata, "account metadata should match")
		}
	})

	t.Run("success ensure account exists", func(t *testing.T) {
		addr := "test:account:4"

		err := store.EnsureAccountExists(context.Background(), addr)
		require.NoError(t, err, "account insertion should not fail")

		account, err := store.GetAccount(context.Background(), addr)
		require.NoError(t, err, "account retrieval should not fail")

		require.Equal(t, addr, account.Address, "account address should match")
	})

	t.Run("success ensure multiple accounts exist", func(t *testing.T) {
		addrs := []string{"test:account:4", "test:account:5", "test:account:6"}

		err := store.EnsureAccountsExist(context.Background(), addrs)
		require.NoError(t, err, "account insertion should not fail")

		for _, addr := range addrs {
			account, err := store.GetAccount(context.Background(), addr)
			require.NoError(t, err, "account retrieval should not fail")

			require.Equal(t, addr, account.Address, "account address should match")
		}
	})
}
