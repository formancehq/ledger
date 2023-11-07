package ledgerstore_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/query"
	internaltesting "github.com/formancehq/ledger/internal/testing"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

func TestGetBalancesAggregated(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()

	tx1 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "users:1", "USD", big.NewInt(1)),
		ledger.NewPosting("world", "users:2", "USD", big.NewInt(199)),
	).WithDate(now)

	tx2 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "users:1", "USD", big.NewInt(1)),
		ledger.NewPosting("world", "users:2", "USD", big.NewInt(199)),
	).WithDate(now.Add(time.Minute)).WithIDUint64(1)

	require.NoError(t, store.InsertLogs(context.Background(),
		ledger.ChainLogs(
			ledger.NewTransactionLog(tx1, map[string]metadata.Metadata{}).WithDate(tx1.Timestamp),
			ledger.NewTransactionLog(tx2, map[string]metadata.Metadata{}).WithDate(tx2.Timestamp),
		)...))

	t.Run("aggregate on all", func(t *testing.T) {
		q := ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilter{}).WithPageSize(10)
		cursor, err := store.GetAggregatedBalances(context.Background(), ledgerstore.NewGetAggregatedBalancesQuery(q))
		require.NoError(t, err)
		internaltesting.RequireEqual(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0),
		}, cursor)
	})
	t.Run("filter on address", func(t *testing.T) {
		ret, err := store.GetAggregatedBalances(context.Background(), ledgerstore.NewGetAggregatedBalancesQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilter{}).
			WithQueryBuilder(query.Match("address", "users:")).
			WithPageSize(10),
		))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{
			"USD": big.NewInt(400),
		}, ret)
	})
	t.Run("using pit", func(t *testing.T) {
		ret, err := store.GetAggregatedBalances(context.Background(), ledgerstore.NewGetAggregatedBalancesQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilter{
			PIT: &now,
		}).
			WithQueryBuilder(query.Match("address", "users:")).
			WithPageSize(10)))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{
			"USD": big.NewInt(200),
		}, ret)
	})
}
