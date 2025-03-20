//go:build it

package legacy_test

import (
	"github.com/formancehq/go-libs/v2/pointer"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger/legacy"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/logging"

	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
)

func TestVolumesList(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	previousPIT := now.Add(-2 * time.Minute)
	futurPIT := now.Add(2 * time.Minute)

	previousOOT := now.Add(-2 * time.Minute)
	futurOOT := now.Add(2 * time.Minute)

	require.NoError(t, store.newStore.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
		"account:1": {
			"category": "1",
		},
		"account:2": {
			"category": "2",
		},
		"world": {
			"foo": "bar",
		},
	}))

	tx1 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
		WithTimestamp(now.Add(-4 * time.Minute)).
		WithInsertedAt(now.Add(4 * time.Minute))
	err := store.newStore.CommitTransaction(ctx, &tx1, nil)
	require.NoError(t, err)

	tx2 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
		WithTimestamp(now.Add(-3 * time.Minute)).
		WithInsertedAt(now.Add(3 * time.Minute))
	err = store.newStore.CommitTransaction(ctx, &tx2, nil)
	require.NoError(t, err)

	tx3 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("account:1", "bank", "USD", big.NewInt(50))).
		WithTimestamp(now.Add(-2 * time.Minute)).
		WithInsertedAt(now.Add(2 * time.Minute))
	err = store.newStore.CommitTransaction(ctx, &tx3, nil)
	require.NoError(t, err)

	tx4 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(0))).
		WithTimestamp(now.Add(-time.Minute)).
		WithInsertedAt(now.Add(time.Minute))
	err = store.newStore.CommitTransaction(ctx, &tx4, nil)
	require.NoError(t, err)

	tx5 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:2", "USD", big.NewInt(50))).
		WithTimestamp(now).
		WithInsertedAt(now)
	err = store.newStore.CommitTransaction(ctx, &tx5, nil)
	require.NoError(t, err)

	tx6 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:2", "USD", big.NewInt(50))).
		WithTimestamp(now.Add(1 * time.Minute)).
		WithInsertedAt(now.Add(-time.Minute))
	err = store.newStore.CommitTransaction(ctx, &tx6, nil)
	require.NoError(t, err)

	tx7 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("account:2", "bank", "USD", big.NewInt(50))).
		WithTimestamp(now.Add(2 * time.Minute)).
		WithInsertedAt(now.Add(-2 * time.Minute))
	err = store.newStore.CommitTransaction(ctx, &tx7, nil)
	require.NoError(t, err)

	tx8 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:2", "USD", big.NewInt(25))).
		WithTimestamp(now.Add(3 * time.Minute)).
		WithInsertedAt(now.Add(-3 * time.Minute))
	err = store.newStore.CommitTransaction(ctx, &tx8, nil)
	require.NoError(t, err)

	t.Run("Get all volumes with balance for insertion date", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.FiltersForVolumes{UseInsertionDate: true})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get all volumes with balance for effective date", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(ledgerstore.FiltersForVolumes{UseInsertionDate: false})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get all volumes with balance for insertion date with previous pit", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgerstore.FiltersForVolumes{
				PITFilter:        ledgerstore.PITFilter{PIT: &previousPIT, OOT: nil},
				UseInsertionDate: true,
			})))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 3)
		require.Equal(t, ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account:2",
			Asset:   "USD",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(25),
				Output:  big.NewInt(50),
				Balance: big.NewInt(-25),
			},
		}, volumes.Data[0])
	})

	t.Run("Get all volumes with balance for insertion date with futur pit", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgerstore.FiltersForVolumes{
				PITFilter:        ledgerstore.PITFilter{PIT: &futurPIT, OOT: nil},
				UseInsertionDate: true,
			})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get all volumes with balance for insertion date with previous oot", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgerstore.FiltersForVolumes{
				PITFilter:        ledgerstore.PITFilter{PIT: nil, OOT: &previousOOT},
				UseInsertionDate: true,
			})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get all volumes with balance for insertion date with future oot", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgerstore.FiltersForVolumes{
				PITFilter:        ledgerstore.PITFilter{PIT: nil, OOT: &futurOOT},
				UseInsertionDate: true,
			})))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 3)
		require.Equal(t, ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account:1",
			Asset:   "USD",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(200),
				Output:  big.NewInt(50),
				Balance: big.NewInt(150),
			},
		}, volumes.Data[0])
	})

	t.Run("Get all volumes with balance for effective date with previous pit", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgerstore.FiltersForVolumes{
				PITFilter:        ledgerstore.PITFilter{PIT: &previousPIT, OOT: nil},
				UseInsertionDate: false,
			})))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 3)
		require.Equal(t, ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account:1",
			Asset:   "USD",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(200),
				Output:  big.NewInt(50),
				Balance: big.NewInt(150),
			},
		}, volumes.Data[0])
	})

	t.Run("Get all volumes with balance for effective date with futur pit", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgerstore.FiltersForVolumes{
				PITFilter:        ledgerstore.PITFilter{PIT: &futurPIT, OOT: nil},
				UseInsertionDate: false,
			})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get all volumes with balance for effective date with previous oot", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgerstore.FiltersForVolumes{
				PITFilter:        ledgerstore.PITFilter{PIT: nil, OOT: &previousOOT},
				UseInsertionDate: false,
			})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get all volumes with balance for effective date with futur oot", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgerstore.FiltersForVolumes{
				PITFilter:        ledgerstore.PITFilter{PIT: nil, OOT: &futurOOT},
				UseInsertionDate: false,
			})))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 3)
		require.Equal(t, ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account:2",
			Asset:   "USD",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(25),
				Output:  big.NewInt(50),
				Balance: big.NewInt(-25),
			},
		}, volumes.Data[0])
	})

	t.Run("Get all volumes with balance for insertion date with future PIT and now OOT", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgerstore.FiltersForVolumes{
				PITFilter:        ledgerstore.PITFilter{PIT: &futurPIT, OOT: &now},
				UseInsertionDate: true,
			})))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 4)
		require.Equal(t, ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account:1",
			Asset:   "USD",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(0),
				Output:  big.NewInt(50),
				Balance: big.NewInt(-50),
			},
		}, volumes.Data[0])

	})

	t.Run("Get all volumes with balance for insertion date with previous OOT and now PIT", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgerstore.FiltersForVolumes{
				PITFilter:        ledgerstore.PITFilter{PIT: &now, OOT: &previousOOT},
				UseInsertionDate: true,
			})))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 3)
		require.Equal(t, ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account:2",
			Asset:   "USD",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(100),
				Output:  big.NewInt(50),
				Balance: big.NewInt(50),
			},
		}, volumes.Data[0])

	})

	t.Run("Get all volumes with balance for effective date with future PIT and now OOT", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgerstore.FiltersForVolumes{
				PITFilter:        ledgerstore.PITFilter{PIT: &futurPIT, OOT: &now},
				UseInsertionDate: false,
			})))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 3)
		require.Equal(t, ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account:2",
			Asset:   "USD",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(100),
				Output:  big.NewInt(50),
				Balance: big.NewInt(50),
			},
		}, volumes.Data[0])
	})

	t.Run("Get all volumes with balance for insertion date with previous OOT and now PIT", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgerstore.FiltersForVolumes{
				PITFilter:        ledgerstore.PITFilter{PIT: &now, OOT: &previousOOT},
				UseInsertionDate: false,
			})))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 4)
		require.Equal(t, ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account:1",
			Asset:   "USD",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(0),
				Output:  big.NewInt(50),
				Balance: big.NewInt(-50),
			},
		}, volumes.Data[0])

	})

	t.Run("Get account1 volume and Balance for insertion date with previous OOT and now PIT", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx,
			ledgerstore.NewGetVolumesWithBalancesQuery(
				ledgercontroller.NewPaginatedQueryOptions(
					ledgerstore.FiltersForVolumes{
						PITFilter:        ledgerstore.PITFilter{PIT: &now, OOT: &previousOOT},
						UseInsertionDate: false,
					}).WithQueryBuilder(query.Match("account", "account:1"))),
		)

		require.NoError(t, err)
		require.Len(t, volumes.Data, 1)
		require.Equal(t, ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account:1",
			Asset:   "USD",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(0),
				Output:  big.NewInt(50),
				Balance: big.NewInt(-50),
			},
		}, volumes.Data[0])

	})

	t.Run("Using Metadata regex", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx,
			ledgerstore.NewGetVolumesWithBalancesQuery(
				ledgercontroller.NewPaginatedQueryOptions(
					ledgerstore.FiltersForVolumes{}).WithQueryBuilder(query.Match("metadata[foo]", "bar"))),
		)

		require.NoError(t, err)
		require.Len(t, volumes.Data, 1)

	})

	t.Run("Using exists metadata filter 1", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx,
			ledgerstore.NewGetVolumesWithBalancesQuery(
				ledgercontroller.NewPaginatedQueryOptions(
					ledgerstore.FiltersForVolumes{}).WithQueryBuilder(query.Exists("metadata", "category"))),
		)

		require.NoError(t, err)
		require.Len(t, volumes.Data, 2)
	})

	t.Run("Using exists metadata filter 2", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx,
			ledgerstore.NewGetVolumesWithBalancesQuery(
				ledgercontroller.NewPaginatedQueryOptions(
					ledgerstore.FiltersForVolumes{}).WithQueryBuilder(query.Exists("metadata", "foo"))),
		)

		require.NoError(t, err)
		require.Len(t, volumes.Data, 1)
	})
}

func TestVolumesAggregate(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	pit := now.Add(2 * time.Minute)
	oot := now.Add(-2 * time.Minute)

	tx1 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1:2", "USD", big.NewInt(100))).
		WithTimestamp(now.Add(-4 * time.Minute)).
		WithInsertedAt(now)
	err := store.newStore.CommitTransaction(ctx, &tx1, nil)
	require.NoError(t, err)

	tx2 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1:1", "EUR", big.NewInt(100))).
		WithTimestamp(now.Add(-3 * time.Minute))
	err = store.newStore.CommitTransaction(ctx, &tx2, nil)
	require.NoError(t, err)

	tx3 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1:2", "EUR", big.NewInt(50))).
		WithTimestamp(now.Add(-2 * time.Minute))
	err = store.newStore.CommitTransaction(ctx, &tx3, nil)
	require.NoError(t, err)

	tx4 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1:3", "USD", big.NewInt(0))).
		WithTimestamp(now.Add(-time.Minute))
	err = store.newStore.CommitTransaction(ctx, &tx4, nil)
	require.NoError(t, err)

	tx5 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:2:1", "USD", big.NewInt(50))).
		WithTimestamp(now)
	err = store.newStore.CommitTransaction(ctx, &tx5, nil)
	require.NoError(t, err)

	tx6 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:2:2", "USD", big.NewInt(50))).
		WithTimestamp(now.Add(1 * time.Minute))
	err = store.newStore.CommitTransaction(ctx, &tx6, nil)
	require.NoError(t, err)

	tx7 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:2:3", "EUR", big.NewInt(25))).
		WithTimestamp(now.Add(3 * time.Minute))
	err = store.newStore.CommitTransaction(ctx, &tx7, nil)
	require.NoError(t, err)

	require.NoError(t, store.newStore.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
		"account:1:1": {
			"foo": "bar",
		},
	}))

	t.Run("Aggregation Volumes with balance for GroupLvl 0", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgerstore.FiltersForVolumes{
					UseInsertionDate: true,
					GroupLvl:         0,
				}).WithQueryBuilder(query.Match("account", "account::"))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 7)
	})

	t.Run("Aggregation Volumes with balance for GroupLvl 1", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgerstore.FiltersForVolumes{
					UseInsertionDate: true,
					GroupLvl:         1,
				}).WithQueryBuilder(query.Match("account", "account::"))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 2)
	})

	t.Run("Aggregation Volumes with balance for GroupLvl 2", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgerstore.FiltersForVolumes{
					UseInsertionDate: true,
					GroupLvl:         2,
				}).WithQueryBuilder(query.Match("account", "account::"))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 4)
	})

	t.Run("Aggregation Volumes with balance for GroupLvl 3", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgerstore.FiltersForVolumes{
					UseInsertionDate: true,
					GroupLvl:         3,
				}).WithQueryBuilder(query.Match("account", "account::"))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 7)
	})

	t.Run("Aggregation Volumes with balance for GroupLvl 1 && PIT && OOT && effectiveDate", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgerstore.FiltersForVolumes{
					PITFilter: ledgerstore.PITFilter{
						PIT: &pit,
						OOT: &oot,
					},
					GroupLvl: 1,
				}).WithQueryBuilder(query.Match("account", "account::"))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 2)
		require.Equal(t, volumes.Data[0], ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account",
			Asset:   "EUR",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(50),
				Output:  big.NewInt(0),
				Balance: big.NewInt(50),
			},
		})
		require.Equal(t, volumes.Data[1], ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account",
			Asset:   "USD",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(100),
				Output:  big.NewInt(0),
				Balance: big.NewInt(100),
			},
		})
	})

	t.Run("Aggregation Volumes with balance for GroupLvl 1 && PIT && OOT && effectiveDate && Balance Filter 1", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgerstore.FiltersForVolumes{
					PITFilter: ledgerstore.PITFilter{
						PIT: &pit,
						OOT: &oot,
					},
					UseInsertionDate: false,
					GroupLvl:         1,
				}).WithQueryBuilder(
				query.And(query.Match("account", "account::"), query.Gte("balance[EUR]", 50)))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 1)
		require.Equal(t, volumes.Data[0], ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account",
			Asset:   "EUR",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(50),
				Output:  big.NewInt(0),
				Balance: big.NewInt(50),
			},
		})
	})

	t.Run("Aggregation Volumes with balance for GroupLvl 1  && Balance Filter 2", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgerstore.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgerstore.FiltersForVolumes{
					PITFilter:        ledgerstore.PITFilter{},
					UseInsertionDate: true,
					GroupLvl:         2,
				}).WithQueryBuilder(
				query.Or(
					query.Match("account", "account:1:"),
					query.Lte("balance[USD]", 0)))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 3)
		require.Equal(t, volumes.Data[0], ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account:1",
			Asset:   "EUR",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(150),
				Output:  big.NewInt(0),
				Balance: big.NewInt(150),
			},
		})
		require.Equal(t, volumes.Data[1], ledger.VolumesWithBalanceByAssetByAccount{
			Account: "account:1",
			Asset:   "USD",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(100),
				Output:  big.NewInt(0),
				Balance: big.NewInt(100),
			},
		})
		require.Equal(t, volumes.Data[2], ledger.VolumesWithBalanceByAssetByAccount{
			Account: "world",
			Asset:   "USD",
			VolumesWithBalance: ledger.VolumesWithBalance{
				Input:   big.NewInt(0),
				Output:  big.NewInt(200),
				Balance: big.NewInt(-200),
			},
		})
	})
	t.Run("filter using account matching, metadata, and group", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx,
			ledgerstore.NewGetVolumesWithBalancesQuery(
				ledgercontroller.NewPaginatedQueryOptions(
					ledgerstore.FiltersForVolumes{
						GroupLvl: 1,
					}).WithQueryBuilder(query.And(
					query.Match("account", "account::"),
					query.Match("metadata[foo]", "bar"),
				))),
		)

		require.NoError(t, err)
		require.Len(t, volumes.Data, 1)
	})

	t.Run("filter using account matching, metadata, and group and PIT", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx,
			ledgerstore.NewGetVolumesWithBalancesQuery(
				ledgercontroller.NewPaginatedQueryOptions(
					ledgerstore.FiltersForVolumes{
						GroupLvl: 1,
						PITFilter: ledgerstore.PITFilter{
							PIT: pointer.For(now.Add(time.Minute)),
						},
					}).WithQueryBuilder(query.And(
					query.Match("account", "account::"),
					query.Match("metadata[foo]", "bar"),
				))),
		)

		require.NoError(t, err)
		require.Len(t, volumes.Data, 1)
	})

	t.Run("filter using metadata matching only", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx,
			ledgerstore.NewGetVolumesWithBalancesQuery(
				ledgercontroller.NewPaginatedQueryOptions(
					ledgerstore.FiltersForVolumes{
						GroupLvl: 1,
					}).WithQueryBuilder(query.And(
					query.Match("metadata[foo]", "bar"),
				))),
		)

		require.NoError(t, err)
		require.Len(t, volumes.Data, 1)
	})
}
