package ledgerstore_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/errors"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

var now = core.Now()
var tx1 = core.ExpandedTransaction{
	Transaction: core.Transaction{
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      big.NewInt(100),
					Asset:       "USD",
				},
			},
			Reference: "tx1",
			Timestamp: now.Add(-3 * time.Hour),
		},
	},
	PostCommitVolumes: core.AccountsAssetsVolumes{
		"world": {
			"USD": {
				Input:  big.NewInt(0),
				Output: big.NewInt(100),
			},
		},
		"central_bank": {
			"USD": {
				Input:  big.NewInt(100),
				Output: big.NewInt(0),
			},
		},
	},
	PreCommitVolumes: core.AccountsAssetsVolumes{
		"world": {
			"USD": {
				Input:  big.NewInt(0),
				Output: big.NewInt(0),
			},
		},
		"central_bank": {
			"USD": {
				Input:  big.NewInt(0),
				Output: big.NewInt(0),
			},
		},
	},
}
var tx2 = core.ExpandedTransaction{
	Transaction: core.Transaction{
		ID: 1,
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      big.NewInt(100),
					Asset:       "USD",
				},
			},
			Reference: "tx2",
			Timestamp: now.Add(-2 * time.Hour),
		},
	},
	PostCommitVolumes: core.AccountsAssetsVolumes{
		"world": {
			"USD": {
				Input:  big.NewInt(0),
				Output: big.NewInt(200),
			},
		},
		"central_bank": {
			"USD": {
				Input:  big.NewInt(200),
				Output: big.NewInt(0),
			},
		},
	},
	PreCommitVolumes: core.AccountsAssetsVolumes{
		"world": {
			"USD": {
				Input:  big.NewInt(0),
				Output: big.NewInt(100),
			},
		},
		"central_bank": {
			"USD": {
				Input:  big.NewInt(100),
				Output: big.NewInt(0),
			},
		},
	},
}
var tx3 = core.ExpandedTransaction{
	Transaction: core.Transaction{
		ID: 2,
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "central_bank",
					Destination: "users:1",
					Amount:      big.NewInt(1),
					Asset:       "USD",
				},
			},
			Reference: "tx3",
			Metadata: metadata.Metadata{
				"priority": "high",
			},
			Timestamp: now.Add(-1 * time.Hour),
		},
	},
	PreCommitVolumes: core.AccountsAssetsVolumes{
		"central_bank": {
			"USD": {
				Input:  big.NewInt(200),
				Output: big.NewInt(0),
			},
		},
		"users:1": {
			"USD": {
				Input:  big.NewInt(0),
				Output: big.NewInt(0),
			},
		},
	},
	PostCommitVolumes: core.AccountsAssetsVolumes{
		"central_bank": {
			"USD": {
				Input:  big.NewInt(200),
				Output: big.NewInt(1),
			},
		},
		"users:1": {
			"USD": {
				Input:  big.NewInt(1),
				Output: big.NewInt(0),
			},
		},
	},
}

func TestUpdateTransactionMetadata(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	tx := core.ExpandedTransaction{
		Transaction: core.Transaction{
			ID: 0,
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					},
				},
				Reference: "foo",
				Timestamp: core.Now(),
			},
		},
	}
	err := store.InsertTransactions(context.Background(), tx)
	require.NoError(t, err)

	err = store.UpdateTransactionMetadata(context.Background(), tx.ID, metadata.Metadata{
		"foo": "bar",
	})
	require.NoError(t, err)

	retrievedTransaction, err := store.GetTransaction(context.Background(), tx.ID)
	require.NoError(t, err)
	require.EqualValues(t, "bar", retrievedTransaction.Metadata["foo"])
}

func TestUpdateAccountMetadata(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	require.NoError(t, store.EnsureAccountExists(context.Background(), "central_bank"))

	err := store.UpdateAccountMetadata(context.Background(), "central_bank", metadata.Metadata{
		"foo": "bar",
	})
	require.NoError(t, err)

	account, err := store.GetAccount(context.Background(), "central_bank")
	require.NoError(t, err)
	require.EqualValues(t, "bar", account.Metadata["foo"])
}

func TestGetAccountNotFound(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	account, err := store.GetAccount(context.Background(), "account_not_existing")
	require.True(t, errors.IsNotFoundError(err))
	require.Nil(t, account)
}

func TestCountAccounts(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	require.NoError(t, store.EnsureAccountExists(context.Background(), "world"))
	require.NoError(t, store.EnsureAccountExists(context.Background(), "central_bank"))

	countAccounts, err := store.CountAccounts(context.Background(), ledgerstore.AccountsQuery{})
	require.NoError(t, err)
	require.EqualValues(t, 2, countAccounts) // world + central_bank
}

func TestGetAssetsVolumes(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	require.NoError(t, store.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
		"central_bank": {
			"USD": {
				Input:  big.NewInt(100),
				Output: big.NewInt(0),
			},
		},
	}))

	volumes, err := store.GetAssetsVolumes(context.Background(), "central_bank")
	require.NoError(t, err)
	require.Len(t, volumes, 1)
	require.EqualValues(t, big.NewInt(100), volumes["USD"].Input)
	require.EqualValues(t, big.NewInt(0), volumes["USD"].Output)
}

func TestGetAccounts(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	require.NoError(t, store.UpdateAccountMetadata(context.Background(), "world", metadata.Metadata{
		"foo": "bar",
	}))
	require.NoError(t, store.UpdateAccountMetadata(context.Background(), "bank", metadata.Metadata{
		"hello": "world",
	}))
	require.NoError(t, store.UpdateAccountMetadata(context.Background(), "order:1", metadata.Metadata{
		"hello": "world",
	}))
	require.NoError(t, store.UpdateAccountMetadata(context.Background(), "order:2", metadata.Metadata{
		"number":  `3`,
		"boolean": `true`,
		"a":       `{"super": {"nested": {"key": "hello"}}}`,
	}))

	accounts, err := store.GetAccounts(context.Background(),
		ledgerstore.NewAccountsQuery().WithPageSize(1),
	)
	require.NoError(t, err)
	require.Equal(t, 1, accounts.PageSize)
	require.Len(t, accounts.Data, 1)

	accounts, err = store.GetAccounts(context.Background(),
		ledgerstore.NewAccountsQuery().
			WithPageSize(1).
			WithAfterAddress(accounts.Data[0].Address),
	)
	require.NoError(t, err)
	require.Equal(t, 1, accounts.PageSize)

	accounts, err = store.GetAccounts(context.Background(),
		ledgerstore.NewAccountsQuery().
			WithPageSize(10).
			WithAddressFilter(".*der.*"),
	)
	require.NoError(t, err)
	require.Len(t, accounts.Data, 2)
	require.Equal(t, 10, accounts.PageSize)

	accounts, err = store.GetAccounts(context.Background(),
		ledgerstore.NewAccountsQuery().
			WithPageSize(10).
			WithMetadataFilter(metadata.Metadata{
				"foo": "bar",
			}),
	)
	require.NoError(t, err)
	require.Len(t, accounts.Data, 1)

	accounts, err = store.GetAccounts(context.Background(), ledgerstore.NewAccountsQuery().
		WithPageSize(10).
		WithMetadataFilter(metadata.Metadata{
			"number": "3",
		}))
	require.NoError(t, err)
	require.Len(t, accounts.Data, 1)

	accounts, err = store.GetAccounts(context.Background(),
		ledgerstore.NewAccountsQuery().
			WithPageSize(10).
			WithMetadataFilter(metadata.Metadata{
				"boolean": "true",
			}),
	)
	require.NoError(t, err)
	require.Len(t, accounts.Data, 1)
}

func TestGetTransaction(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	require.NoError(t, store.InsertTransactions(context.Background(), tx1, tx2))

	tx, err := store.GetTransaction(context.Background(), tx1.ID)
	require.NoError(t, err)
	require.Equal(t, tx1.Postings, tx.Postings)
	require.Equal(t, tx1.Reference, tx.Reference)
	require.Equal(t, tx1.Timestamp, tx.Timestamp)
}

func TestInitializeStore(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	modified, err := store.Migrate(context.Background())
	require.NoError(t, err)
	require.False(t, modified)
}
