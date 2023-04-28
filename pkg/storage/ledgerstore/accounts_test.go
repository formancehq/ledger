package ledgerstore_test

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBalanceFromLogs(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	const batchNumber = 100
	const batchSize = 10
	const input = 100
	const output = 10

	logs := make([]*core.ActiveLog, 0)
	for i := 0; i < batchNumber; i++ {
		for j := 0; j < batchSize; j++ {
			activeLog := core.NewActiveLog(core.NewTransactionLog(
				core.NewTransaction().WithPostings(
					core.NewPosting("world", fmt.Sprintf("account:%d", j), "EUR/2", big.NewInt(input)),
					core.NewPosting(fmt.Sprintf("account:%d", j), "starbucks", "EUR/2", big.NewInt(output)),
				).WithID(uint64(i*batchSize+j)),
				map[string]metadata.Metadata{},
			))
			logs = append(logs, activeLog)
		}
	}
	_, err := store.InsertLogs(context.Background(), logs)
	require.NoError(t, err)

	balance, err := store.GetBalanceFromLogs(context.Background(), "account:1", "EUR/2")
	require.NoError(t, err)
	require.Equal(t, big.NewInt((input-output)*batchNumber), balance)
}

func TestGetMetadataFromLogs(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	logs := make([]*core.ActiveLog, 0)
	logs = append(logs, core.NewActiveLog(core.NewTransactionLog(
		core.NewTransaction().WithPostings(
			core.NewPosting("world", "bank", "EUR/2", big.NewInt(100)),
			core.NewPosting("bank", "starbucks", "EUR/2", big.NewInt(10)),
		),
		map[string]metadata.Metadata{},
	)))
	logs = append(logs, core.NewActiveLog(core.NewSetMetadataLog(core.Now(), core.SetMetadataLogPayload{
		TargetType: core.MetaTargetTypeAccount,
		TargetID:   "bank",
		Metadata: metadata.Metadata{
			"foo": "bar",
		},
	})))
	logs = append(logs, core.NewActiveLog(core.NewTransactionLog(
		core.NewTransaction().WithPostings(
			core.NewPosting("world", "bank", "EUR/2", big.NewInt(100)),
			core.NewPosting("bank", "starbucks", "EUR/2", big.NewInt(10)),
		).WithID(1),
		map[string]metadata.Metadata{},
	)))
	logs = append(logs, core.NewActiveLog(core.NewSetMetadataLog(core.Now(), core.SetMetadataLogPayload{
		TargetType: core.MetaTargetTypeAccount,
		TargetID:   "bank",
		Metadata: metadata.Metadata{
			"role": "admin",
		},
	})))
	logs = append(logs, core.NewActiveLog(core.NewTransactionLog(
		core.NewTransaction().WithPostings(
			core.NewPosting("world", "bank", "EUR/2", big.NewInt(100)),
			core.NewPosting("bank", "starbucks", "EUR/2", big.NewInt(10)),
		).WithID(2),
		map[string]metadata.Metadata{},
	)))

	_, err := store.InsertLogs(context.Background(), logs)
	require.NoError(t, err)

	metadata, err := store.GetMetadataFromLogs(context.Background(), "bank", "foo")
	require.NoError(t, err)
	require.Equal(t, "bar", metadata)
}

func TestAccounts(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	t.Run("success balance", func(t *testing.T) {
		q := ledgerstore.NewAccountsQuery().
			WithPageSize(10).
			WithBalanceFilter("50")

		_, err := store.GetAccounts(context.Background(), q)
		assert.NoError(t, err, "balance filter should not fail")
	})

	t.Run("panic invalid balance", func(t *testing.T) {
		q := ledgerstore.NewAccountsQuery().
			WithPageSize(10).
			WithBalanceFilter("TEST")

		assert.PanicsWithError(
			t, `invalid balance parameter: strconv.ParseInt: parsing "TEST": invalid syntax`,

			func() {
				_, _ = store.GetAccounts(context.Background(), q)
			}, "invalid balance in storage should panic")
	})

	t.Run("panic invalid balance operator", func(t *testing.T) {
		assert.PanicsWithValue(t, "invalid balance operator parameter", func() {
			q := ledgerstore.NewAccountsQuery().
				WithPageSize(10).
				WithBalanceFilter("50").
				WithBalanceOperatorFilter("TEST")

			_, _ = store.GetAccounts(context.Background(), q)
		}, "invalid balance operator in storage should panic")
	})

	t.Run("success balance operator", func(t *testing.T) {
		q := ledgerstore.NewAccountsQuery().
			WithPageSize(10).
			WithBalanceFilter("50").
			WithBalanceOperatorFilter(ledgerstore.BalanceOperatorLte)

		_, err := store.GetAccounts(context.Background(), q)
		assert.NoError(t, err, "balance operator filter should not fail")
	})

	t.Run("success account insertion", func(t *testing.T) {
		addr := "test:account"
		metadata := metadata.Metadata{
			"foo": "bar",
		}

		err := store.UpdateAccountMetadata(context.Background(), addr, metadata)
		assert.NoError(t, err, "account insertion should not fail")

		account, err := store.GetAccount(context.Background(), addr)
		assert.NoError(t, err, "account retrieval should not fail")

		assert.Equal(t, addr, account.Address, "account address should match")
		assert.Equal(t, metadata, account.Metadata, "account metadata should match")
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
		assert.NoError(t, err, "account insertion should not fail")

		for _, account := range accounts {
			acc, err := store.GetAccount(context.Background(), account.Address)
			assert.NoError(t, err, "account retrieval should not fail")

			assert.Equal(t, account.Address, acc.Address, "account address should match")
			assert.Equal(t, account.Metadata, acc.Metadata, "account metadata should match")
		}
	})

	t.Run("success ensure account exists", func(t *testing.T) {
		addr := "test:account:4"

		err := store.EnsureAccountExists(context.Background(), addr)
		assert.NoError(t, err, "account insertion should not fail")

		account, err := store.GetAccount(context.Background(), addr)
		assert.NoError(t, err, "account retrieval should not fail")

		assert.Equal(t, addr, account.Address, "account address should match")
	})

	t.Run("success ensure multiple accounts exist", func(t *testing.T) {
		addrs := []string{"test:account:4", "test:account:5", "test:account:6"}

		err := store.EnsureAccountsExist(context.Background(), addrs)
		assert.NoError(t, err, "account insertion should not fail")

		for _, addr := range addrs {
			account, err := store.GetAccount(context.Background(), addr)
			assert.NoError(t, err, "account retrieval should not fail")

			assert.Equal(t, addr, account.Address, "account address should match")
		}
	})
}
