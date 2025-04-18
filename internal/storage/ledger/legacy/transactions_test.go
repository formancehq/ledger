//go:build it

package legacy_test

import (
	"context"
	"fmt"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger/legacy"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v2/time"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"

	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
)

func TestGetTransactionWithVolumes(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	tx1 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "central_bank", "USD", big.NewInt(100)),
		).
		WithReference("tx1").
		WithTimestamp(now.Add(-3 * time.Hour))
	err := store.newStore.CommitTransaction(ctx, &tx1, nil)
	require.NoError(t, err)

	tx2 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "central_bank", "USD", big.NewInt(100)),
		).
		WithReference("tx2").
		WithTimestamp(now.Add(-2 * time.Hour))
	err = store.newStore.CommitTransaction(ctx, &tx2, nil)
	require.NoError(t, err)

	tx, err := store.GetTransactionWithVolumes(ctx, ledgerstore.NewGetTransactionQuery(*tx1.ID).
		WithExpandVolumes().
		WithExpandEffectiveVolumes())
	require.NoError(t, err)
	require.Equal(t, tx1.Postings, tx.Postings)
	require.Equal(t, tx1.Reference, tx.Reference)
	require.Equal(t, tx1.Timestamp, tx.Timestamp)
	RequireEqual(t, ledger.PostCommitVolumes{
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
	}, tx.PostCommitVolumes)

	tx, err = store.GetTransactionWithVolumes(ctx, ledgerstore.NewGetTransactionQuery(*tx2.ID).
		WithExpandVolumes().
		WithExpandEffectiveVolumes())
	require.NoError(t, err)
	require.Equal(t, tx2.Postings, tx.Postings)
	require.Equal(t, tx2.Reference, tx.Reference)
	require.Equal(t, tx2.Timestamp, tx.Timestamp)
	RequireEqual(t, ledger.PostCommitVolumes{
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
	}, tx.PostCommitVolumes)
}

func TestCountTransactions(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	for i := 0; i < 3; i++ {
		tx := ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", fmt.Sprintf("account%d", i), "USD", big.NewInt(100)),
		)
		err := store.newStore.CommitTransaction(ctx, &tx, nil)
		require.NoError(t, err)
	}

	count, err := store.CountTransactions(context.Background(), ledgerstore.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{})))
	require.NoError(t, err, "counting transactions should not fail")
	require.Equal(t, 3, count, "count should be equal")
}

func TestGetTransactions(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	tx1 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "alice", "USD", big.NewInt(100)),
		).
		WithMetadata(metadata.Metadata{"category": "1"}).
		WithTimestamp(now.Add(-3 * time.Hour))
	err := store.newStore.CommitTransaction(ctx, &tx1, nil)
	require.NoError(t, err)

	tx2 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "bob", "USD", big.NewInt(100)),
		).
		WithMetadata(metadata.Metadata{"category": "2"}).
		WithTimestamp(now.Add(-2 * time.Hour))
	err = store.newStore.CommitTransaction(ctx, &tx2, nil)
	require.NoError(t, err)

	tx3BeforeRevert := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "users:marley", "USD", big.NewInt(100)),
		).
		WithMetadata(metadata.Metadata{"category": "3"}).
		WithTimestamp(now.Add(-time.Hour))
	err = store.newStore.CommitTransaction(ctx, &tx3BeforeRevert, nil)
	require.NoError(t, err)

	_, hasBeenReverted, err := store.newStore.RevertTransaction(ctx, *tx3BeforeRevert.ID, time.Time{})
	require.NoError(t, err)
	require.True(t, hasBeenReverted)

	tx4 := tx3BeforeRevert.Reverse().WithTimestamp(now)
	err = store.newStore.CommitTransaction(ctx, &tx4, nil)
	require.NoError(t, err)

	_, _, err = store.newStore.UpdateTransactionMetadata(ctx, *tx3BeforeRevert.ID, metadata.Metadata{
		"additional_metadata": "true",
	}, time.Time{})
	require.NoError(t, err)

	// refresh tx3
	// we can't take the result of the call on RevertTransaction nor UpdateTransactionMetadata as the result does not contains pc(e)v
	tx3 := func() ledger.Transaction {
		tx3, err := store.Store.GetTransactionWithVolumes(ctx, ledgerstore.NewGetTransactionQuery(*tx3BeforeRevert.ID).
			WithExpandVolumes().
			WithExpandEffectiveVolumes())
		require.NoError(t, err)
		return *tx3
	}()

	tx5 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("users:marley", "sellers:amazon", "USD", big.NewInt(100)),
		).
		WithTimestamp(now)
	err = store.newStore.CommitTransaction(ctx, &tx5, nil)
	require.NoError(t, err)

	type testCase struct {
		name        string
		query       ledgercontroller.PaginatedQueryOptions[ledgerstore.PITFilterWithVolumes]
		expected    []ledger.Transaction
		expectError error
	}
	testCases := []testCase{
		{
			name:     "nominal",
			query:    ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}),
			expected: []ledger.Transaction{tx5, tx4, tx3, tx2, tx1},
		},
		{
			name: "address filter",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("account", "bob")),
			expected: []ledger.Transaction{tx2},
		},
		{
			name: "address filter using segments matching two addresses by individual segments",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("account", "users:amazon")),
			expected: []ledger.Transaction{},
		},
		{
			name: "address filter using segment",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("account", "users:")),
			expected: []ledger.Transaction{tx5, tx4, tx3},
		},
		{
			name: "filter using metadata",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("metadata[category]", "2")),
			expected: []ledger.Transaction{tx2},
		},
		{
			name: "using point in time",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: pointer.For(now.Add(-time.Hour)),
				},
			}),
			expected: []ledger.Transaction{tx3BeforeRevert, tx2, tx1},
		},
		{
			name: "reverted transactions",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("reverted", true)),
			expected: []ledger.Transaction{tx3},
		},
		{
			name: "filter using exists metadata",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Exists("metadata", "category")),
			expected: []ledger.Transaction{tx3, tx2, tx1},
		},
		{
			name: "filter using metadata and pit",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: pointer.For(tx3.Timestamp),
				},
			}).
				WithQueryBuilder(query.Match("metadata[category]", "2")),
			expected: []ledger.Transaction{tx2},
		},
		{
			name: "filter using not exists metadata",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Not(query.Exists("metadata", "category"))),
			expected: []ledger.Transaction{tx5, tx4},
		},
		{
			name: "filter using timestamp",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("timestamp", tx5.Timestamp.Format(time.RFC3339Nano))),
			expected: []ledger.Transaction{tx5, tx4},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.query.Options.ExpandVolumes = true
			tc.query.Options.ExpandEffectiveVolumes = true

			cursor, err := store.GetTransactions(ctx, ledgerstore.NewListTransactionsQuery(tc.query))
			if tc.expectError != nil {
				require.True(t, errors.Is(err, tc.expectError))
			} else {
				require.NoError(t, err)
				require.Len(t, cursor.Data, len(tc.expected))
				RequireEqual(t, tc.expected, cursor.Data)

				count, err := store.CountTransactions(ctx, ledgerstore.NewListTransactionsQuery(tc.query))
				require.NoError(t, err)

				require.EqualValues(t, len(tc.expected), count)
			}
		})
	}
}
