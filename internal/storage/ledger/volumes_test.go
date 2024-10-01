//go:build it

package ledger_test

import (
	"database/sql"
	"math/big"
	"testing"
	libtime "time"

	"errors"
	"github.com/formancehq/go-libs/platform/postgres"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

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

	require.NoError(t, store.UpdateAccountsMetadata(ctx, map[string]metadata.Metadata{
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
	err := store.CommitTransaction(ctx, &tx1)
	require.NoError(t, err)

	tx2 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
		WithTimestamp(now.Add(-3 * time.Minute)).
		WithInsertedAt(now.Add(3 * time.Minute))
	err = store.CommitTransaction(ctx, &tx2)
	require.NoError(t, err)

	tx3 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("account:1", "bank", "USD", big.NewInt(50))).
		WithTimestamp(now.Add(-2 * time.Minute)).
		WithInsertedAt(now.Add(2 * time.Minute))
	err = store.CommitTransaction(ctx, &tx3)
	require.NoError(t, err)

	tx4 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(0))).
		WithTimestamp(now.Add(-time.Minute)).
		WithInsertedAt(now.Add(time.Minute))
	err = store.CommitTransaction(ctx, &tx4)
	require.NoError(t, err)

	tx5 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:2", "USD", big.NewInt(50))).
		WithTimestamp(now).
		WithInsertedAt(now)
	err = store.CommitTransaction(ctx, &tx5)
	require.NoError(t, err)

	tx6 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:2", "USD", big.NewInt(50))).
		WithTimestamp(now.Add(1 * time.Minute)).
		WithInsertedAt(now.Add(-time.Minute))
	err = store.CommitTransaction(ctx, &tx6)
	require.NoError(t, err)

	tx7 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("account:2", "bank", "USD", big.NewInt(50))).
		WithTimestamp(now.Add(2 * time.Minute)).
		WithInsertedAt(now.Add(-2 * time.Minute))
	err = store.CommitTransaction(ctx, &tx7)
	require.NoError(t, err)

	tx8 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:2", "USD", big.NewInt(25))).
		WithTimestamp(now.Add(3 * time.Minute)).
		WithInsertedAt(now.Add(-3 * time.Minute))
	err = store.CommitTransaction(ctx, &tx8)
	require.NoError(t, err)

	//require.NoError(t, store.InsertLogs(ctx,
	//	ledger.ChainLogs(
	//		//ledger.NewSetMetadataOnAccountLog(time.Now(), "account:1", metadata.Metadata{"category": "1"}).WithTimestamp(now),
	//		//ledger.NewSetMetadataOnAccountLog(time.Now(), "account:2", metadata.Metadata{"category": "2"}).WithTimestamp(now),
	//		//ledger.NewSetMetadataOnAccountLog(time.Now(), "world", metadata.Metadata{"foo": "bar"}).WithTimestamp(now),
	//		//ledger.NewLog(
	//		//	ledger.NewTransaction().
	//		//		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
	//		//		WithTimestamp(now.Add(-4*time.Minute)),
	//		//	map[string]metadata.Metadata{},
	//		//).WithTimestamp(now.Add(4*time.Minute)),
	//		//
	//		//ledger.NewLog(
	//		//	ledger.NewTransaction().
	//		//		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(100))).
	//		//		WithIDUint64(1).
	//		//		WithTimestamp(now.Add(-3*time.Minute)),
	//		//	map[string]metadata.Metadata{},
	//		//).WithTimestamp(now.Add(3*time.Minute)),
	//
	//		//ledger.NewLog(
	//		//	ledger.NewTransaction().
	//		//		WithPostings(ledger.NewPosting("account:1", "bank", "USD", big.NewInt(50))).
	//		//		WithTimestamp(now.Add(-2*time.Minute)).
	//		//		WithIDUint64(2),
	//		//	map[string]metadata.Metadata{},
	//		//).WithTimestamp(now.Add(2*time.Minute)),
	//
	//		//ledger.NewLog(
	//		//	ledger.NewTransaction().
	//		//		WithPostings(ledger.NewPosting("world", "account:1", "USD", big.NewInt(0))).
	//		//		WithTimestamp(now.Add(-time.Minute)).
	//		//		WithIDUint64(3),
	//		//	map[string]metadata.Metadata{},
	//		//).WithTimestamp(now.Add(1*time.Minute)),
	//
	//		//ledger.NewLog(
	//		//	ledger.NewTransaction().
	//		//		WithPostings(ledger.NewPosting("world", "account:2", "USD", big.NewInt(50))).
	//		//		WithTimestamp(now).WithIDUint64(4),
	//		//	map[string]metadata.Metadata{},
	//		//).WithTimestamp(now),
	//
	//		//ledger.NewLog(
	//		//	ledger.NewTransaction().
	//		//		WithPostings(ledger.NewPosting("world", "account:2", "USD", big.NewInt(50))).
	//		//		WithIDUint64(5).
	//		//		WithTimestamp(now.Add(1*time.Minute)),
	//		//	map[string]metadata.Metadata{},
	//		//).WithTimestamp(now.Add(-1*time.Minute)),
	//
	//		//ledger.NewLog(
	//		//	ledger.NewTransaction().
	//		//		WithPostings(ledger.NewPosting("account:2", "bank", "USD", big.NewInt(50))).
	//		//		WithTimestamp(now.Add(2*time.Minute)).
	//		//		WithIDUint64(6),
	//		//	map[string]metadata.Metadata{},
	//		//).WithTimestamp(now.Add(-2*time.Minute)),
	//
	//		//ledger.NewLog(
	//		//	ledger.NewTransaction().
	//		//		WithPostings(ledger.NewPosting("world", "account:2", "USD", big.NewInt(25))).
	//		//		WithTimestamp(now.Add(3*time.Minute)).
	//		//		WithIDUint64(7),
	//		//	map[string]metadata.Metadata{},
	//		//).WithTimestamp(now.Add(-3*time.Minute)),
	//	)...,
	//))

	t.Run("Get All Volumes with Balance for Insertion date", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.FiltersForVolumes{UseInsertionDate: true})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get All Volumes with Balance for Effective date", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.FiltersForVolumes{UseInsertionDate: false})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get All Volumes with Balance for Insertion date with previous pit", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgercontroller.FiltersForVolumes{
				PITFilter:        ledgercontroller.PITFilter{PIT: &previousPIT, OOT: nil},
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
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgercontroller.FiltersForVolumes{
				PITFilter:        ledgercontroller.PITFilter{PIT: &futurPIT, OOT: nil},
				UseInsertionDate: true,
			})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get All Volumes with Balance for Insertion date with previous oot", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgercontroller.FiltersForVolumes{
				PITFilter:        ledgercontroller.PITFilter{PIT: nil, OOT: &previousOOT},
				UseInsertionDate: true,
			})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get All Volumes with Balance for Insertion date with futur oot", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgercontroller.FiltersForVolumes{
				PITFilter:        ledgercontroller.PITFilter{PIT: nil, OOT: &futurOOT},
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
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgercontroller.FiltersForVolumes{
				PITFilter:        ledgercontroller.PITFilter{PIT: &previousPIT, OOT: nil},
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
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgercontroller.FiltersForVolumes{
				PITFilter:        ledgercontroller.PITFilter{PIT: &futurPIT, OOT: nil},
				UseInsertionDate: false,
			})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get All Volumes with Balance for Effective date with previous oot", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgercontroller.FiltersForVolumes{
				PITFilter:        ledgercontroller.PITFilter{PIT: nil, OOT: &previousOOT},
				UseInsertionDate: false,
			})))
		require.NoError(t, err)

		require.Len(t, volumes.Data, 4)
	})

	t.Run("Get All Volumes with Balance for effective date with futur oot", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgercontroller.FiltersForVolumes{
				PITFilter:        ledgercontroller.PITFilter{PIT: nil, OOT: &futurOOT},
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
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgercontroller.FiltersForVolumes{
				PITFilter:        ledgercontroller.PITFilter{PIT: &futurPIT, OOT: &now},
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
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgercontroller.FiltersForVolumes{
				PITFilter:        ledgercontroller.PITFilter{PIT: &now, OOT: &previousOOT},
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
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgercontroller.FiltersForVolumes{
				PITFilter:        ledgercontroller.PITFilter{PIT: &futurPIT, OOT: &now},
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
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(ledgercontroller.NewPaginatedQueryOptions(
			ledgercontroller.FiltersForVolumes{
				PITFilter:        ledgercontroller.PITFilter{PIT: &now, OOT: &previousOOT},
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
			ledgercontroller.NewGetVolumesWithBalancesQuery(
				ledgercontroller.NewPaginatedQueryOptions(
					ledgercontroller.FiltersForVolumes{
						PITFilter:        ledgercontroller.PITFilter{PIT: &now, OOT: &previousOOT},
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
			ledgercontroller.NewGetVolumesWithBalancesQuery(
				ledgercontroller.NewPaginatedQueryOptions(
					ledgercontroller.FiltersForVolumes{}).WithQueryBuilder(query.Match("metadata[foo]", "bar"))),
		)

		require.NoError(t, err)
		require.Len(t, volumes.Data, 1)

	})

	t.Run("Using exists metadata filter 1", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx,
			ledgercontroller.NewGetVolumesWithBalancesQuery(
				ledgercontroller.NewPaginatedQueryOptions(
					ledgercontroller.FiltersForVolumes{}).WithQueryBuilder(query.Exists("metadata", "category"))),
		)

		require.NoError(t, err)
		require.Len(t, volumes.Data, 2)

	})

	t.Run("Using exists metadata filter 2", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx,
			ledgercontroller.NewGetVolumesWithBalancesQuery(
				ledgercontroller.NewPaginatedQueryOptions(
					ledgercontroller.FiltersForVolumes{}).WithQueryBuilder(query.Exists("metadata", "foo"))),
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

	pit := now.Add(2 * time.Minute)
	oot := now.Add(-2 * time.Minute)

	tx1 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1:2", "USD", big.NewInt(100))).
		WithTimestamp(now.Add(-4 * time.Minute)).
		WithInsertedAt(now)
	err := store.CommitTransaction(ctx, &tx1)
	require.NoError(t, err)

	tx2 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1:1", "EUR", big.NewInt(100))).
		WithTimestamp(now.Add(-3 * time.Minute))
	err = store.CommitTransaction(ctx, &tx2)
	require.NoError(t, err)

	tx3 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1:2", "EUR", big.NewInt(50))).
		WithTimestamp(now.Add(-2 * time.Minute))
	err = store.CommitTransaction(ctx, &tx3)
	require.NoError(t, err)

	tx4 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:1:3", "USD", big.NewInt(0))).
		WithTimestamp(now.Add(-time.Minute))
	err = store.CommitTransaction(ctx, &tx4)
	require.NoError(t, err)

	tx5 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:2:1", "USD", big.NewInt(50))).
		WithTimestamp(now)
	err = store.CommitTransaction(ctx, &tx5)
	require.NoError(t, err)

	tx6 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:2:2", "USD", big.NewInt(50))).
		WithTimestamp(now.Add(1 * time.Minute))
	err = store.CommitTransaction(ctx, &tx6)
	require.NoError(t, err)

	tx7 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "account:2:3", "EUR", big.NewInt(25))).
		WithTimestamp(now.Add(3 * time.Minute))
	err = store.CommitTransaction(ctx, &tx7)
	require.NoError(t, err)

	t.Run("Aggregation Volumes with Balance for GroupLvl 0", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgercontroller.FiltersForVolumes{
					UseInsertionDate: true,
					GroupLvl:         0,
				}).WithQueryBuilder(query.Match("account", "account::"))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 7)
	})

	t.Run("Aggregation Volumes with Balance for GroupLvl 1", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgercontroller.FiltersForVolumes{
					UseInsertionDate: true,
					GroupLvl:         1,
				}).WithQueryBuilder(query.Match("account", "account::"))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 2)
	})

	t.Run("Aggregation Volumes with Balance for GroupLvl 2", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgercontroller.FiltersForVolumes{
					UseInsertionDate: true,
					GroupLvl:         2,
				}).WithQueryBuilder(query.Match("account", "account::"))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 4)
	})

	t.Run("Aggregation Volumes with Balance for GroupLvl 3", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgercontroller.FiltersForVolumes{
					UseInsertionDate: true,
					GroupLvl:         3,
				}).WithQueryBuilder(query.Match("account", "account::"))))

		require.NoError(t, err)
		require.Len(t, volumes.Data, 7)
	})

	t.Run("Aggregation Volumes with Balance for GroupLvl 1 && PIT && OOT && effectiveDate", func(t *testing.T) {
		t.Parallel()

		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgercontroller.FiltersForVolumes{
					PITFilter: ledgercontroller.PITFilter{
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

	t.Run("Aggregation Volumes with Balance for GroupLvl 1 && PIT && OOT && effectiveDate && Balance Filter 1", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgercontroller.FiltersForVolumes{
					PITFilter: ledgercontroller.PITFilter{
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

	t.Run("Aggregation Volumes with Balance for GroupLvl 1  && Balance Filter 2", func(t *testing.T) {
		t.Parallel()
		volumes, err := store.GetVolumesWithBalances(ctx, ledgercontroller.NewGetVolumesWithBalancesQuery(
			ledgercontroller.NewPaginatedQueryOptions(
				ledgercontroller.FiltersForVolumes{
					PITFilter:        ledgercontroller.PITFilter{},
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
}

func TestUpdateVolumes(t *testing.T) {
	t.Parallel()

	t.Run("update volumes of same account sequentially", func(t *testing.T) {
		t.Parallel()

		store := newLedgerStore(t)
		ctx := logging.TestingContext()

		volumes, err := store.UpdateVolumes(ctx, ledger.AccountsVolumes{
			Account: "world",
			Asset:   "USD/2",
			Input:   big.NewInt(0),
			Output:  big.NewInt(100),
		})
		require.NoError(t, err)
		require.Equal(t, ledger.PostCommitVolumes{
			"world": {
				"USD/2": ledger.NewVolumesInt64(0, 100),
			},
		}, volumes)

		volumes, err = store.UpdateVolumes(ctx, ledger.AccountsVolumes{
			Account: "world",
			Asset:   "USD/2",
			Input:   big.NewInt(50),
			Output:  big.NewInt(0),
		})
		require.NoError(t, err)
		require.Equal(t, ledger.PostCommitVolumes{
			"world": {
				"USD/2": ledger.NewVolumesInt64(50, 100),
			},
		}, volumes)

		volumes, err = store.UpdateVolumes(ctx, ledger.AccountsVolumes{
			Account: "world",
			Asset:   "USD/2",
			Input:   big.NewInt(50),
			Output:  big.NewInt(50),
		})
		require.NoError(t, err)
		require.Equal(t, ledger.PostCommitVolumes{
			"world": {
				"USD/2": ledger.NewVolumesInt64(100, 150),
			},
		}, volumes)
	})

	t.Run("get balance of not existing account should take a lock", func(t *testing.T) {
		t.Parallel()

		store := newLedgerStore(t)
		ctx := logging.TestingContext()

		sqlTx1, err := store.GetDB().BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = sqlTx1.Rollback()
		})
		storeTx1 := store.WithDB(sqlTx1)

		sqlTx2, err := store.GetDB().BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = sqlTx2.Rollback()
		})
		storeTx2 := store.WithDB(sqlTx2)

		// At this stage, the accounts_volumes table is empty.
		// Take balance of the 'world' account should force a lock.
		volumes, err := storeTx1.GetBalances(ctx, ledgercontroller.BalanceQuery{
			"world": {"USD"},
		})
		require.NoError(t, err)
		require.Equal(t, ledgercontroller.Balances{
			"world": {
				"USD": big.NewInt(0),
			},
		}, volumes)

		// Take an advisory lock on tx2
		_, err = storeTx2.GetDB().NewRaw(`select pg_advisory_xact_lock(1)`).Exec(ctx)
		require.NoError(t, err)

		errChan := make(chan error, 2)
		go func() {
			// This call should block as the lock for the row holding 'world' balance is owned by tx1
			_, err := storeTx2.GetBalances(ctx, ledgercontroller.BalanceQuery{
				"world": {"USD"},
			})
			errChan <- err
		}()

		go func() {
			// Take the same advisory lock for tx1 as tx2.
			// As tx1 hold a lock on the world balance, and tx2 is waiting for that balance,
			// it should trigger a deadlock.
			_, err = storeTx1.GetDB().NewRaw(`select pg_advisory_xact_lock(1)`).Exec(ctx)
			errChan <- postgres.ResolveError(err)
		}()

		// Either tx1 or tx2 should be cancelled by PG with a deadlock error
		select {
		case err := <-errChan:
			if err == nil {
				select {
				case err = <-errChan:
					if err == nil {
						require.Fail(t, "should have a deadlock")
					}
				case <-libtime.After(2 * time.Second):
					require.Fail(t, "transaction should have finished")
				}
			}
			require.True(t, errors.Is(err, postgres.ErrDeadlockDetected))
		case <-libtime.After(2 * time.Second):
			require.Fail(t, "transaction should have finished")
		}
	})
}
