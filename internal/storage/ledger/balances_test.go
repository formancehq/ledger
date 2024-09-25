//go:build it

package ledger

import (
	"database/sql"
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/time"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/pointer"

	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	libtime "time"
)

func TestGetBalances(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	world := &Account{
		Ledger:                     store.ledger.Name,
		Address:                    "world",
		AddressArray:               []string{"world"},
		InsertionDate:              time.Now(),
		UpdatedAt:                  time.Now(),
		FirstUsage:                 time.Now(),
	}
	_, err := store.upsertAccount(ctx, world)
	require.NoError(t, err)

	_, err = store.updateVolumes(ctx, AccountsVolumes{
		Ledger:    store.ledger.Name,
		Account:   "world",
		Asset:     "USD",
		Inputs:  new(big.Int),
		Outputs: big.NewInt(100),
		AccountsSeq: world.Seq,
	})
	require.NoError(t, err)

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
			// 500ms seems ok. I don't like to play with times in tests, but I don't know how to do otherwise
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
	})
}

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
	err := store.CommitTransaction(ctx, &tx1)
	require.NoError(t, err)

	tx2 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "users:1", "USD", bigInt),
			ledger.NewPosting("world", "users:2", "USD", smallInt),
			ledger.NewPosting("world", "xxx", "EUR", smallInt),
		).
		WithTimestamp(now.Add(-time.Minute)).
		WithInsertedAt(now.Add(time.Minute))
	err = store.CommitTransaction(ctx, &tx2)
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

func TestUpdateBalances(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	world := &Account{
		Ledger:                     store.ledger.Name,
		Address:                    "world",
		AddressArray:               []string{"world"},
		InsertionDate:              time.Now(),
		UpdatedAt:                  time.Now(),
		FirstUsage:                 time.Now(),
	}
	_, err := store.upsertAccount(ctx, world)
	require.NoError(t, err)

	volumes, err := store.updateVolumes(ctx, AccountsVolumes{
		Ledger:    store.ledger.Name,
		Account:   "world",
		Asset:     "USD/2",
		Inputs:    big.NewInt(0),
		Outputs:   big.NewInt(100),
		AccountsSeq: world.Seq,
	})
	require.NoError(t, err)
	require.Equal(t, map[string]map[string]ledger.Volumes{
		"world": {
			"USD/2": ledger.NewVolumesInt64(0, 100),
		},
	}, volumes)

	volumes, err = store.updateVolumes(ctx, AccountsVolumes{
		Ledger:    store.ledger.Name,
		Account:   "world",
		Asset:     "USD/2",
		Inputs:    big.NewInt(50),
		Outputs:   big.NewInt(0),
	})
	require.NoError(t, err)
	require.Equal(t, map[string]map[string]ledger.Volumes{
		"world": {
			"USD/2": ledger.NewVolumesInt64(50, 100),
		},
	}, volumes)

	volumes, err = store.updateVolumes(ctx, AccountsVolumes{
		Ledger:    store.ledger.Name,
		Account:   "world",
		Asset:     "USD/2",
		Inputs:    big.NewInt(50),
		Outputs:   big.NewInt(50),
		AccountsSeq: world.Seq,
	})
	require.NoError(t, err)
	require.Equal(t, map[string]map[string]ledger.Volumes{
		"world": {
			"USD/2": ledger.NewVolumesInt64(100, 150),
		},
	}, volumes)
}
