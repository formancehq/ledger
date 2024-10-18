//go:build it

package ledgerstore

import (
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/google/go-cmp/cmp"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"

	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
)

func TestGetBalancesAggregated(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	bigInt, _ := big.NewInt(0).SetString("1000", 10)
	smallInt := big.NewInt(100)

	tx1 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "users:1", "USD", bigInt),
			ledger.NewPosting("world", "users:2", "USD", smallInt),
		).
		WithTimestamp(now).
		WithInsertedAt(now)
	err := store.newStore.CommitTransaction(ctx, &tx1)
	require.NoError(t, err)

	tx2 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "users:1", "USD", bigInt),
			ledger.NewPosting("world", "users:2", "USD", smallInt),
			ledger.NewPosting("world", "xxx", "EUR", smallInt),
		).
		WithTimestamp(now.Add(-time.Minute)).
		WithInsertedAt(now.Add(time.Minute))
	err = store.newStore.CommitTransaction(ctx, &tx2)
	require.NoError(t, err)

	require.NoError(t, store.newStore.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
		"users:1": {
			"category": "premium",
		},
		"users:2": {
			"category": "premium",
		},
	}))

	require.NoError(t, store.newStore.DeleteAccountMetadata(ctx, "users:2", "category"))

	require.NoError(t, store.newStore.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
		"users:1": {
			"category": "premium",
		},
		"users:2": {
			"category": "2",
		},
		"world": {
			"world": "bar",
		},
	}))

	t.Run("aggregate on all", func(t *testing.T) {
		t.Parallel()
		cursor, err := store.GetAggregatedBalances(ctx, ledgercontroller.NewGetAggregatedBalancesQuery(ledgercontroller.PITFilter{}, nil, false))
		require.NoError(t, err)
		RequireEqual(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0),
			"EUR": big.NewInt(0),
		}, cursor)
	})
	t.Run("filter on address", func(t *testing.T) {
		t.Parallel()
		ret, err := store.GetAggregatedBalances(ctx, ledgercontroller.NewGetAggregatedBalancesQuery(ledgercontroller.PITFilter{},
			query.Match("address", "users:"), false))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0).Add(
				big.NewInt(0).Mul(bigInt, big.NewInt(2)),
				big.NewInt(0).Mul(smallInt, big.NewInt(2)),
			),
		}, ret)
	})
	t.Run("using pit on effective date", func(t *testing.T) {
		t.Parallel()
		ret, err := store.GetAggregatedBalances(ctx, ledgercontroller.NewGetAggregatedBalancesQuery(ledgercontroller.PITFilter{
			PIT: pointer.For(now.Add(-time.Second)),
		}, query.Match("address", "users:"), false))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0).Add(
				bigInt,
				smallInt,
			),
		}, ret)
	})
	t.Run("using pit on insertion date", func(t *testing.T) {
		t.Parallel()
		ret, err := store.GetAggregatedBalances(ctx, ledgercontroller.NewGetAggregatedBalancesQuery(ledgercontroller.PITFilter{
			PIT: pointer.For(now),
		}, query.Match("address", "users:"), true))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0).Add(
				bigInt,
				smallInt,
			),
		}, ret)
	})
	t.Run("using a metadata and pit", func(t *testing.T) {
		t.Parallel()

		ret, err := store.GetAggregatedBalances(ctx, ledgercontroller.NewGetAggregatedBalancesQuery(ledgercontroller.PITFilter{
			PIT: pointer.For(now.Add(time.Minute)),
		}, query.Match("metadata[category]", "premium"), false))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0).Add(
				big.NewInt(0).Mul(bigInt, big.NewInt(2)),
				big.NewInt(0),
			),
		}, ret)
	})
	t.Run("using a metadata without pit", func(t *testing.T) {
		t.Parallel()
		ret, err := store.GetAggregatedBalances(ctx, ledgercontroller.NewGetAggregatedBalancesQuery(ledgercontroller.PITFilter{},
			query.Match("metadata[category]", "premium"), false))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0).Mul(bigInt, big.NewInt(2)),
		}, ret)
	})
	t.Run("when no matching", func(t *testing.T) {
		t.Parallel()
		ret, err := store.GetAggregatedBalances(ctx, ledgercontroller.NewGetAggregatedBalancesQuery(ledgercontroller.PITFilter{},
			query.Match("metadata[category]", "guest"), false))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{}, ret)
	})

	t.Run("using a filter exist on metadata", func(t *testing.T) {
		t.Parallel()
		ret, err := store.GetAggregatedBalances(ctx, ledgercontroller.NewGetAggregatedBalancesQuery(ledgercontroller.PITFilter{}, query.Exists("metadata", "category"), false))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0).Add(
				big.NewInt(0).Mul(bigInt, big.NewInt(2)),
				big.NewInt(0).Mul(smallInt, big.NewInt(2)),
			),
		}, ret)
	})
}

func RequireEqual(t *testing.T, expected, actual any) {
	t.Helper()
	if diff := cmp.Diff(expected, actual, cmp.Comparer(bigIntComparer)); diff != "" {
		require.Failf(t, "Content not matching", diff)
	}
}

func bigIntComparer(v1 *big.Int, v2 *big.Int) bool {
	return v1.String() == v2.String()
}
