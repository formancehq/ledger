package ledgerstore_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	internaltesting "github.com/formancehq/ledger/internal/testing"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/stretchr/testify/require"
)

func TestGetBalancesAggregated(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()

	bigInt, _ := big.NewInt(0).SetString("999999999999999999999999999999999999999999999999999999999999999999999999999999999", 10)
	smallInt := big.NewInt(199)

	tx1 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "users:1", "USD", bigInt),
		ledger.NewPosting("world", "users:2", "USD", smallInt),
	).WithDate(now)

	tx2 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "users:1", "USD", bigInt),
		ledger.NewPosting("world", "users:2", "USD", smallInt),
	).WithDate(now.Add(time.Minute)).WithIDUint64(1)

	require.NoError(t, store.InsertLogs(context.Background(),
		ledger.ChainLogs(
			ledger.NewTransactionLog(tx1, map[string]metadata.Metadata{}).WithDate(tx1.Timestamp),
			ledger.NewTransactionLog(tx2, map[string]metadata.Metadata{}).WithDate(tx2.Timestamp),
		)...))

	t.Run("aggregate on all", func(t *testing.T) {
		t.Parallel()
		q := ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilter{}).WithPageSize(10)
		cursor, err := store.GetAggregatedBalances(context.Background(), ledgerstore.NewGetAggregatedBalancesQuery(q))
		require.NoError(t, err)
		internaltesting.RequireEqual(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0),
		}, cursor)
	})
	t.Run("filter on address", func(t *testing.T) {
		t.Parallel()
		ret, err := store.GetAggregatedBalances(context.Background(), ledgerstore.NewGetAggregatedBalancesQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilter{}).
			WithQueryBuilder(query.Match("address", "users:")).
			WithPageSize(10),
		))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0).Add(
				big.NewInt(0).Mul(bigInt, big.NewInt(2)),
				big.NewInt(0).Mul(smallInt, big.NewInt(2)),
			),
		}, ret)
	})
	t.Run("using pit", func(t *testing.T) {
		t.Parallel()
		ret, err := store.GetAggregatedBalances(context.Background(), ledgerstore.NewGetAggregatedBalancesQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilter{
			PIT: &now,
		}).
			WithQueryBuilder(query.Match("address", "users:")).
			WithPageSize(10)))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0).Add(
				bigInt,
				smallInt,
			),
		}, ret)
	})
}
