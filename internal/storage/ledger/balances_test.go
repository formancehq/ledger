//go:build it

package ledger_test

import (
	"database/sql"
	"github.com/formancehq/ledger/internal/storage/common"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/time"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"

	libtime "time"

	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
)

func TestBalancesGet(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	world := &ledger.Account{
		Address:       "world",
		InsertionDate: time.Now(),
		UpdatedAt:     time.Now(),
		FirstUsage:    time.Now(),
	}
	err := store.UpsertAccounts(ctx, world)
	require.NoError(t, err)

	_, err = store.UpdateVolumes(ctx, ledger.AccountsVolumes{
		Account: "world",
		Asset:   "USD",
		Input:   new(big.Int),
		Output:  big.NewInt(100),
	})
	require.NoError(t, err)

	t.Run("get balances of not existing account should create an empty row", func(t *testing.T) {
		t.Parallel()

		balances, err := store.GetBalances(ctx, ledgercontroller.BalanceQuery{
			"orders:1234": []string{"USD"},
		})
		require.NoError(t, err)
		require.Len(t, balances, 1)
		require.NotNil(t, balances["orders:1234"])
		require.Len(t, balances["orders:1234"], 1)
		require.Equal(t, big.NewInt(0), balances["orders:1234"]["USD"])

		volumes := make([]*ledger.AccountsVolumes, 0)

		err = store.GetDB().NewSelect().
			Model(&volumes).
			ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
			Where("accounts_address = ?", "orders:1234").
			Scan(ctx)
		require.NoError(t, err)
		require.Len(t, volumes, 1)
		require.Equal(t, "USD", volumes[0].Asset)
		require.Equal(t, big.NewInt(0), volumes[0].Input)
		require.Equal(t, big.NewInt(0), volumes[0].Output)
	})

	t.Run("check concurrent access on same balance", func(t *testing.T) {
		t.Parallel()

		tx1, err := store.GetDB().BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = tx1.Rollback()
		})
		store1 := store.WithDB(tx1)

		tx2, err := store.GetDB().BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = tx2.Rollback()
		})
		store2 := store.WithDB(tx2)

		bq := ledgercontroller.BalanceQuery{
			"world": []string{"USD"},
		}

		balances, err := store1.GetBalances(ctx, bq)
		require.NoError(t, err)
		require.Len(t, balances, 1)

		getBalancesAccepted := make(chan struct{})
		go func() {
			_, err := store2.GetBalances(ctx, bq)
			require.NoError(t, err)
			close(getBalancesAccepted)
		}()

		select {
		case <-libtime.After(500 * time.Millisecond):
			// notes(gfyrag): Wait for 500ms to ensure the parallel tx does not have the ability to update balances
			// of the already taken accounts.
			// 500ms seems ok. I need to find another way to not relying on time, it's brittle.
		case <-getBalancesAccepted:
			t.Fatalf("parallel tx should not have been blocked")
		}

		require.NoError(t, tx1.Commit())

		select {
		case <-libtime.After(100 * time.Millisecond):
			t.Fatalf("parallel tx should have been unlocked")
		case <-getBalancesAccepted:
		}
	})

	t.Run("balance query with empty balance", func(t *testing.T) {

		tx, err := store.GetDB().BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, tx.Rollback())
		})

		store := store.WithDB(tx)

		count, err := store.GetDB().NewSelect().
			ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
			Where("ledger = ?", store.GetLedger().Name).
			Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, count)

		balances, err := store.GetBalances(ctx, ledgercontroller.BalanceQuery{
			"world":        {"USD"},
			"not-existing": {"USD"},
		})
		require.NoError(t, err)
		require.Len(t, balances, 2)
		require.NotNil(t, balances["world"])
		require.NotNil(t, balances["not-existing"])

		require.Equal(t, big.NewInt(-100), balances["world"]["USD"])
		require.Equal(t, big.NewInt(0), balances["not-existing"]["USD"])

		count, err = store.GetDB().NewSelect().
			ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
			Where("ledger = ?", store.GetLedger().Name).
			Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, count)
	})
}

func TestBalancesAggregates(t *testing.T) {
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
	err := store.CommitTransaction(ctx, &tx1, nil)
	require.NoError(t, err)

	tx2 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "users:1", "USD", bigInt),
			ledger.NewPosting("world", "users:2", "USD", smallInt),
			ledger.NewPosting("world", "xxx", "EUR", smallInt),
		).
		WithTimestamp(now.Add(-time.Minute)).
		WithInsertedAt(now.Add(time.Minute))
	err = store.CommitTransaction(ctx, &tx2, nil)
	require.NoError(t, err)

	require.NoError(t, store.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
		"users:1": {
			"category": "premium",
		},
		"users:2": {
			"category": "premium",
		},
	}))

	require.NoError(t, store.DeleteAccountMetadata(ctx, "users:2", "category"))

	require.NoError(t, store.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
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

		ret, err := store.AggregatedVolumes().GetOne(ctx, common.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{})
		require.NoError(t, err)
		RequireEqual(t, ledger.AggregatedVolumes{
			Aggregated: ledger.VolumesByAssets{
				"USD": ledger.Volumes{
					Input: big.NewInt(0).Add(
						big.NewInt(0).Mul(bigInt, big.NewInt(2)),
						big.NewInt(0).Mul(smallInt, big.NewInt(2)),
					),
					Output: big.NewInt(0).Add(
						big.NewInt(0).Mul(bigInt, big.NewInt(2)),
						big.NewInt(0).Mul(smallInt, big.NewInt(2)),
					),
				},
				"EUR": ledger.Volumes{
					Input:  smallInt,
					Output: smallInt,
				},
			},
		}, *ret)
	})
	t.Run("filter on address", func(t *testing.T) {
		t.Parallel()

		ret, err := store.AggregatedVolumes().GetOne(ctx, common.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
			Builder: query.Match("address", "users:"),
		})
		require.NoError(t, err)
		RequireEqual(t, ledger.AggregatedVolumes{
			Aggregated: ledger.VolumesByAssets{
				"USD": ledger.Volumes{
					Input: big.NewInt(0).Add(
						big.NewInt(0).Mul(bigInt, big.NewInt(2)),
						big.NewInt(0).Mul(smallInt, big.NewInt(2)),
					),
					Output: new(big.Int),
				},
			},
		}, *ret)
	})
	t.Run("using pit on effective date", func(t *testing.T) {
		t.Parallel()
		ret, err := store.AggregatedVolumes().GetOne(ctx, common.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
			Builder: query.Match("address", "users:"),
			PIT:     pointer.For(now.Add(-time.Second)),
		})
		require.NoError(t, err)
		RequireEqual(t, ledger.AggregatedVolumes{
			Aggregated: ledger.VolumesByAssets{
				"USD": ledger.Volumes{
					Input: big.NewInt(0).Add(
						bigInt,
						smallInt,
					),
					Output: new(big.Int),
				},
			},
		}, *ret)
	})
	t.Run("using pit on insertion date", func(t *testing.T) {
		t.Parallel()
		ret, err := store.AggregatedVolumes().GetOne(ctx, common.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
			Builder: query.Match("address", "users:"),
			PIT:     pointer.For(now),
			Opts: ledgercontroller.GetAggregatedVolumesOptions{
				UseInsertionDate: true,
			},
		})
		require.NoError(t, err)
		RequireEqual(t, ledger.AggregatedVolumes{
			Aggregated: ledger.VolumesByAssets{
				"USD": ledger.Volumes{
					Input: big.NewInt(0).Add(
						bigInt,
						smallInt,
					),
					Output: new(big.Int),
				},
			},
		}, *ret)
	})
	t.Run("using a metadata and pit", func(t *testing.T) {
		t.Parallel()
		ret, err := store.AggregatedVolumes().GetOne(ctx, common.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
			PIT:     pointer.For(now.Add(time.Minute)),
			Builder: query.Match("metadata[category]", "premium"),
		})
		require.NoError(t, err)
		RequireEqual(t, ledger.AggregatedVolumes{
			Aggregated: ledger.VolumesByAssets{
				"USD": ledger.Volumes{
					Input: big.NewInt(0).Add(
						big.NewInt(0).Mul(bigInt, big.NewInt(2)),
						big.NewInt(0),
					),
					Output: new(big.Int),
				},
			},
		}, *ret)
	})
	t.Run("using a metadata without pit", func(t *testing.T) {
		t.Parallel()
		ret, err := store.AggregatedVolumes().GetOne(ctx, common.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
			Builder: query.Match("metadata[category]", "premium"),
		})
		require.NoError(t, err)

		RequireEqual(t, ledger.AggregatedVolumes{
			Aggregated: ledger.VolumesByAssets{
				"USD": ledger.Volumes{
					Input:  big.NewInt(0).Mul(bigInt, big.NewInt(2)),
					Output: new(big.Int),
				},
			},
		}, *ret)
	})
	t.Run("when no matching", func(t *testing.T) {
		t.Parallel()
		ret, err := store.AggregatedVolumes().GetOne(ctx, common.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
			Builder: query.Match("metadata[category]", "guest"),
		})
		require.NoError(t, err)
		RequireEqual(t, ledger.AggregatedVolumes{
			Aggregated: ledger.VolumesByAssets{},
		}, *ret)
	})

	t.Run("using a filter exist on metadata", func(t *testing.T) {
		t.Parallel()
		ret, err := store.AggregatedVolumes().GetOne(ctx, common.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
			Builder: query.Exists("metadata", "category"),
		})
		require.NoError(t, err)
		RequireEqual(t, ledger.AggregatedVolumes{
			Aggregated: ledger.VolumesByAssets{
				"USD": ledger.Volumes{
					Input: big.NewInt(0).Add(
						big.NewInt(0).Mul(bigInt, big.NewInt(2)),
						big.NewInt(0).Mul(smallInt, big.NewInt(2)),
					),
					Output: new(big.Int),
				},
			},
		}, *ret)
	})

	t.Run("using a filter on metadata and on address", func(t *testing.T) {
		t.Parallel()
		ret, err := store.AggregatedVolumes().GetOne(ctx, common.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
			Builder: query.And(
				query.Match("address", "users:"),
				query.Match("metadata[category]", "premium"),
			),
		})
		require.NoError(t, err)
		RequireEqual(t, ledger.AggregatedVolumes{
			Aggregated: ledger.VolumesByAssets{
				"USD": ledger.Volumes{
					Input:  big.NewInt(0).Mul(bigInt, big.NewInt(2)),
					Output: new(big.Int),
				},
			},
		}, *ret)
	})
}
