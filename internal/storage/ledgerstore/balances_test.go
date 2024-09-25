//go:build it

package ledgerstore

import (
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/pointer"

	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/v2/internal"
	internaltesting "github.com/formancehq/ledger/v2/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestGetBalancesAggregated(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	bigInt, _ := big.NewInt(0).SetString("999999999999999999999999999999999999999999999999999999999999999999999999999999999", 10)
	smallInt := big.NewInt(199)

	tx1 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "users:1", "USD", bigInt),
		ledger.NewPosting("world", "users:2", "USD", smallInt),
	).WithDate(now)

	tx2 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "users:1", "USD", bigInt),
		ledger.NewPosting("world", "users:2", "USD", smallInt),
		ledger.NewPosting("world", "xxx", "EUR", smallInt),
	).WithDate(now.Add(-time.Minute)).WithIDUint64(1)

	logs := []*ledger.Log{
		ledger.NewTransactionLog(tx1, map[string]metadata.Metadata{}).WithDate(now),
		ledger.NewTransactionLog(tx2, map[string]metadata.Metadata{}).WithDate(now.Add(time.Minute)),
		ledger.NewSetMetadataLog(now.Add(time.Minute), ledger.SetMetadataLogPayload{
			TargetType: ledger.MetaTargetTypeAccount,
			TargetID:   "users:1",
			Metadata: metadata.Metadata{
				"category": "premium",
			},
		}),
		ledger.NewSetMetadataLog(now.Add(time.Minute), ledger.SetMetadataLogPayload{
			TargetType: ledger.MetaTargetTypeAccount,
			TargetID:   "users:2",
			Metadata: metadata.Metadata{
				"category": "premium",
			},
		}),
		ledger.NewDeleteMetadataLog(now.Add(2*time.Minute), ledger.DeleteMetadataLogPayload{
			TargetType: ledger.MetaTargetTypeAccount,
			TargetID:   "users:2",
			Key:        "category",
		}),
		ledger.NewSetMetadataOnAccountLog(time.Now(), "users:1", metadata.Metadata{"category": "premium"}).WithDate(now.Add(time.Minute)),
		ledger.NewSetMetadataOnAccountLog(time.Now(), "users:2", metadata.Metadata{"category": "2"}).WithDate(now.Add(time.Minute)),
		ledger.NewSetMetadataOnAccountLog(time.Now(), "world", metadata.Metadata{"foo": "bar"}).WithDate(now.Add(time.Minute)),
	}

	require.NoError(t, store.InsertLogs(ctx, ledger.ChainLogs(logs...)...))

	t.Run("aggregate on all", func(t *testing.T) {
		t.Parallel()
		cursor, err := store.GetAggregatedBalances(ctx, NewGetAggregatedBalancesQuery(PITFilter{}, nil, false))
		require.NoError(t, err)
		internaltesting.RequireEqual(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0),
			"EUR": big.NewInt(0),
		}, cursor)
	})
	t.Run("filter on address", func(t *testing.T) {
		t.Parallel()
		ret, err := store.GetAggregatedBalances(ctx, NewGetAggregatedBalancesQuery(PITFilter{},
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
		ret, err := store.GetAggregatedBalances(ctx, NewGetAggregatedBalancesQuery(PITFilter{
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
		ret, err := store.GetAggregatedBalances(ctx, NewGetAggregatedBalancesQuery(PITFilter{
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
		ret, err := store.GetAggregatedBalances(ctx, NewGetAggregatedBalancesQuery(PITFilter{
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
		ret, err := store.GetAggregatedBalances(ctx, NewGetAggregatedBalancesQuery(PITFilter{},
			query.Match("metadata[category]", "premium"), false))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0).Mul(bigInt, big.NewInt(2)),
		}, ret)
	})
	t.Run("when no matching", func(t *testing.T) {
		t.Parallel()
		ret, err := store.GetAggregatedBalances(ctx, NewGetAggregatedBalancesQuery(PITFilter{},
			query.Match("metadata[category]", "guest"), false))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{}, ret)
	})

	t.Run("using a filter exist on metadata", func(t *testing.T) {
		t.Parallel()
		ret, err := store.GetAggregatedBalances(ctx, NewGetAggregatedBalancesQuery(PITFilter{}, query.Exists("metadata", "category"), false))
		require.NoError(t, err)
		require.Equal(t, ledger.BalancesByAssets{
			"USD": big.NewInt(0).Add(
				big.NewInt(0).Mul(bigInt, big.NewInt(2)),
				big.NewInt(0).Mul(smallInt, big.NewInt(2)),
			),
		}, ret)
	})
}
