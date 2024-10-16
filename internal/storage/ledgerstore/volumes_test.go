//go:build it

package ledgerstore

import (
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/logging"

	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
)

func TestGetVolumesWithBalances(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	previousPIT := now.Add(-2 * time.Minute)
	futurPIT := now.Add(2 * time.Minute)

	previousOOT := now.Add(-2 * time.Minute)
	futurOOT := now.Add(2 * time.Minute)

	require.NoError(t, store.InsertLogs(ctx,
		ledger.ChainLogs(
			ledger.NewSetMetadataOnAccountLog(time.Now(), "account:1", metadata.Metadata{"category": "1"}).WithDate(now),
			ledger.NewSetMetadataOnAccountLog(time.Now(), "account:2", metadata.Metadata{"category": "2"}).WithDate(now),
			ledger.NewSetMetadataOnAccountLog(time.Now(), "world", metadata.Metadata{"foo": "bar"}).WithDate(now),
			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
					WithDate(now.Add(-4*time.Minute)),
				map[string]metadata.Metadata{},
			).WithDate(now.Add(4*time.Minute)),

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
					WithIDUint64(1).
					WithDate(now.Add(-3*time.Minute)),
				map[string]metadata.Metadata{},
			).WithDate(now.Add(3*time.Minute)),

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("account:1", "bank", "USD", big.NewInt(50))).
					WithDate(now.Add(-2*time.Minute)).
					WithIDUint64(2),
				map[string]metadata.Metadata{},
			).WithDate(now.Add(2*time.Minute)),

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(0))).
					WithDate(now.Add(-time.Minute)).
					WithIDUint64(3),
				map[string]metadata.Metadata{},
			).WithDate(now.Add(1*time.Minute)),

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:2", "USD", big.NewInt(50))).
					WithDate(now).WithIDUint64(4),
				map[string]metadata.Metadata{},
			).WithDate(now),

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:2", "USD", big.NewInt(50))).
					WithIDUint64(5).
					WithDate(now.Add(1*time.Minute)),
				map[string]metadata.Metadata{},
			).WithDate(now.Add(-1*time.Minute)),

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("account:2", "bank", "USD", big.NewInt(50))).
					WithDate(now.Add(2*time.Minute)).
					WithIDUint64(6),
				map[string]metadata.Metadata{},
			).WithDate(now.Add(-2*time.Minute)),

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:2", "USD", big.NewInt(25))).
					WithDate(now.Add(3*time.Minute)).
					WithIDUint64(7),
				map[string]metadata.Metadata{},
			).WithDate(now.Add(-3*time.Minute)),
		)...,
	))

	t.Run("Get All Volumes with Balance for Insertion date", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(FiltersForVolumes{UseInsertionDate: true})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get All Volumes with Balance for Effective date", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(FiltersForVolumes{UseInsertionDate: false})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get All Volumes with Balance for Insertion date with previous pit", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(
			FiltersForVolumes{
				PITFilter:        PITFilter{PIT: &previousPIT, OOT: nil},
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

	t.Run("Get All Volumes with Balance for Insertion date with futur pit", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(
			FiltersForVolumes{
				PITFilter:        PITFilter{PIT: &futurPIT, OOT: nil},
				UseInsertionDate: true,
			})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get All Volumes with Balance for Insertion date with previous oot", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(
			FiltersForVolumes{
				PITFilter:        PITFilter{PIT: nil, OOT: &previousOOT},
				UseInsertionDate: true,
			})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get All Volumes with Balance for Insertion date with futur oot", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(
			FiltersForVolumes{
				PITFilter:        PITFilter{PIT: nil, OOT: &futurOOT},
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

	t.Run("Get All Volumes with Balance for Effective date with previous pit", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(
			FiltersForVolumes{
				PITFilter:        PITFilter{PIT: &previousPIT, OOT: nil},
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

	t.Run("Get All Volumes with Balance for Effective date with futur pit", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(
			FiltersForVolumes{
				PITFilter:        PITFilter{PIT: &futurPIT, OOT: nil},
				UseInsertionDate: false,
			})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get All Volumes with Balance for Effective date with previous oot", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(
			FiltersForVolumes{
				PITFilter:        PITFilter{PIT: nil, OOT: &previousOOT},
				UseInsertionDate: false,
			})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get All Volumes with Balance for effective date with futur oot", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(
			FiltersForVolumes{
				PITFilter:        PITFilter{PIT: nil, OOT: &futurOOT},
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

	t.Run("Get All Volumes with Balance for insertion date with futur PIT and now OOT", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(
			FiltersForVolumes{
				PITFilter:        PITFilter{PIT: &futurPIT, OOT: &now},
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

	t.Run("Get All Volumes with Balance for insertion date with previous OOT and now PIT", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(
			FiltersForVolumes{
				PITFilter:        PITFilter{PIT: &now, OOT: &previousOOT},
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

	t.Run("Get All Volumes with Balance for effective date with futur PIT and now OOT", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(
			FiltersForVolumes{
				PITFilter:        PITFilter{PIT: &futurPIT, OOT: &now},
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

	t.Run("Get All Volumes with Balance for insertion date with previous OOT and now PIT", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(NewPaginatedQueryOptions(
			FiltersForVolumes{
				PITFilter:        PITFilter{PIT: &now, OOT: &previousOOT},
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
			NewGetVolumesWithBalancesQuery(
				NewPaginatedQueryOptions(
					FiltersForVolumes{
						PITFilter:        PITFilter{PIT: &now, OOT: &previousOOT},
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
			NewGetVolumesWithBalancesQuery(
				NewPaginatedQueryOptions(
					FiltersForVolumes{}).WithQueryBuilder(query.Match("metadata[foo]", "bar"))),
		)

		require.NoError(t, err)
		require.Len(t, volumes.Data, 1)

	})

	t.Run("Using exists metadata filter 1", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx,
			NewGetVolumesWithBalancesQuery(
				NewPaginatedQueryOptions(
					FiltersForVolumes{}).WithQueryBuilder(query.Exists("metadata", "category"))),
		)

		require.NoError(t, err)
		require.Len(t, volumes.Data, 2)

	})

	t.Run("Using exists metadata filter 2", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx,
			NewGetVolumesWithBalancesQuery(
				NewPaginatedQueryOptions(
					FiltersForVolumes{}).WithQueryBuilder(query.Exists("metadata", "foo"))),
		)

		require.NoError(t, err)
		require.Len(t, volumes.Data, 1)
	})
}

func TestAggGetVolumesWithBalances(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	futurPIT := now.Add(2 * time.Minute)
	previousOOT := now.Add(-2 * time.Minute)

	require.NoError(t, store.InsertLogs(ctx,
		ledger.ChainLogs(

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:1:2", "USD", big.NewInt(100))).
					WithDate(now.Add(-4*time.Minute)),
				map[string]metadata.Metadata{},
			).WithDate(now),

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:1:1", "EUR", big.NewInt(100))).
					WithIDUint64(1).
					WithDate(now.Add(-3*time.Minute)),
				map[string]metadata.Metadata{},
			).WithDate(now),

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:1:2", "EUR", big.NewInt(50))).
					WithDate(now.Add(-2*time.Minute)).
					WithIDUint64(2),
				map[string]metadata.Metadata{},
			).WithDate(now),

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:1:3", "USD", big.NewInt(0))).
					WithDate(now.Add(-time.Minute)).
					WithIDUint64(3),
				map[string]metadata.Metadata{},
			).WithDate(now),

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:2:1", "USD", big.NewInt(50))).
					WithDate(now).WithIDUint64(4),
				map[string]metadata.Metadata{},
			).WithDate(now),

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:2:2", "USD", big.NewInt(50))).
					WithIDUint64(5).
					WithDate(now.Add(1*time.Minute)),
				map[string]metadata.Metadata{},
			).WithDate(now),

			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(ledger.NewPosting("world", "account:2:3", "EUR", big.NewInt(25))).
					WithDate(now.Add(3*time.Minute)).
					WithIDUint64(7),
				map[string]metadata.Metadata{},
			).WithDate(now),

			ledger.NewSetMetadataOnAccountLog(time.Now(), "account:1:1", metadata.Metadata{
				"foo": "bar",
			}),
		)...,
	))

	t.Run("Aggregation Volumes with Balance for GroupLvl 0", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(
			NewPaginatedQueryOptions(
				FiltersForVolumes{
					UseInsertionDate: true,
					GroupLvl:         0,
				}).WithQueryBuilder(query.Match("account", "account::"))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 7)
	})

	t.Run("Aggregation Volumes with Balance for GroupLvl 1", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(
			NewPaginatedQueryOptions(
				FiltersForVolumes{
					UseInsertionDate: true,
					GroupLvl:         1,
				}).WithQueryBuilder(query.Match("account", "account::"))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 2)
	})

	t.Run("Aggregation Volumes with Balance for GroupLvl 2", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(
			NewPaginatedQueryOptions(
				FiltersForVolumes{
					UseInsertionDate: true,
					GroupLvl:         2,
				}).WithQueryBuilder(query.Match("account", "account::"))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 4)
	})

	t.Run("Aggregation Volumes with Balance for GroupLvl 3", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(
			NewPaginatedQueryOptions(
				FiltersForVolumes{
					UseInsertionDate: true,
					GroupLvl:         3,
				}).WithQueryBuilder(query.Match("account", "account::"))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 7)
	})

	t.Run("Aggregation Volumes with Balance for GroupLvl 1 && PIT && OOT && effectiveDate", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(
			NewPaginatedQueryOptions(
				FiltersForVolumes{
					PITFilter: PITFilter{
						PIT: &futurPIT,
						OOT: &previousOOT,
					},
					UseInsertionDate: false,
					GroupLvl:         1,
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

	t.Run("Aggregation Volumes with Balance for GroupLvl 1 && PIT && OOT && effectiveDate && Balance Filter 1", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(
			NewPaginatedQueryOptions(
				FiltersForVolumes{
					PITFilter: PITFilter{
						PIT: &futurPIT,
						OOT: &previousOOT,
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

	t.Run("Aggregation Volumes with Balance for GroupLvl 1  && Balance Filter 2", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, NewGetVolumesWithBalancesQuery(
			NewPaginatedQueryOptions(
				FiltersForVolumes{
					PITFilter:        PITFilter{},
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
			NewGetVolumesWithBalancesQuery(
				NewPaginatedQueryOptions(
					FiltersForVolumes{
						GroupLvl: 1,
					}).WithQueryBuilder(query.And(
					query.Match("account", "account::"),
					query.Match("metadata[foo]", "bar"),
				))),
		)

		require.NoError(t, err)
		require.Len(t, volumes.Data, 1)
	})
}
