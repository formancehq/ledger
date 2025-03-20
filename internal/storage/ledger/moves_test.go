//go:build it

package ledger_test

import (
	"database/sql"
	"fmt"
	"math/big"
	"math/rand"
	"testing"

	"errors"
	"github.com/alitto/pond"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
)

func TestMovesInsert(t *testing.T) {
	t.Parallel()

	t.Run("nominal", func(t *testing.T) {
		t.Parallel()

		store := newLedgerStore(t)
		ctx := logging.TestingContext()

		tx := ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
		)
		require.NoError(t, store.InsertTransaction(ctx, &tx))

		account := &ledger.Account{
			Address: "world",
		}
		err := store.UpsertAccounts(ctx, account)
		require.NoError(t, err)

		now := time.Now()

		// We will insert 5 moves at five different timestamps and check than pv(c)e evolves correctly
		// t0 ---------> t1 ---------> t2 ---------> t3 ----------> t4
		// m1 ---------> m3 ---------> m4 ---------> m2 ----------> m5
		t0 := now
		t1 := t0.Add(time.Hour)
		t2 := t1.Add(time.Hour)
		t3 := t2.Add(time.Hour)
		t4 := t3.Add(time.Hour)

		// Insert a first move at t0
		m1 := ledger.Move{
			IsSource:      true,
			Account:       "world",
			Amount:        (*bunpaginate.BigInt)(big.NewInt(100)),
			Asset:         "USD",
			InsertionDate: t0,
			EffectiveDate: t0,
			TransactionID: *tx.ID,
		}
		require.NoError(t, store.InsertMoves(ctx, &m1))
		require.NotNil(t, m1.PostCommitEffectiveVolumes)
		require.Equal(t, ledger.Volumes{
			Input:  big.NewInt(0),
			Output: big.NewInt(100),
		}, *m1.PostCommitEffectiveVolumes)

		// Add a second move at t3
		m2 := ledger.Move{
			IsSource:      false,
			Account:       "world",
			Amount:        (*bunpaginate.BigInt)(big.NewInt(50)),
			Asset:         "USD",
			InsertionDate: t3,
			EffectiveDate: t3,
			TransactionID: *tx.ID,
		}
		require.NoError(t, store.InsertMoves(ctx, &m2))
		require.NotNil(t, m2.PostCommitEffectiveVolumes)
		require.Equal(t, ledger.Volumes{
			Input:  big.NewInt(50),
			Output: big.NewInt(100),
		}, *m2.PostCommitEffectiveVolumes)

		// Add a third move at t1
		m3 := ledger.Move{
			IsSource:      true,
			Account:       "world",
			Amount:        (*bunpaginate.BigInt)(big.NewInt(200)),
			Asset:         "USD",
			InsertionDate: t1,
			EffectiveDate: t1,
			TransactionID: *tx.ID,
		}
		require.NoError(t, store.InsertMoves(ctx, &m3))
		require.NotNil(t, m3.PostCommitEffectiveVolumes)
		require.Equal(t, ledger.Volumes{
			Input:  big.NewInt(0),
			Output: big.NewInt(300),
		}, *m3.PostCommitEffectiveVolumes)

		// Add a fourth move at t2
		m4 := ledger.Move{
			IsSource:      false,
			Account:       "world",
			Amount:        (*bunpaginate.BigInt)(big.NewInt(50)),
			Asset:         "USD",
			InsertionDate: t2,
			EffectiveDate: t2,
			TransactionID: *tx.ID,
		}
		require.NoError(t, store.InsertMoves(ctx, &m4))
		require.NotNil(t, m4.PostCommitEffectiveVolumes)
		require.Equal(t, ledger.Volumes{
			Input:  big.NewInt(50),
			Output: big.NewInt(300),
		}, *m4.PostCommitEffectiveVolumes)

		// Add a fifth move at t4
		m5 := ledger.Move{
			IsSource:      false,
			Account:       "world",
			Amount:        (*bunpaginate.BigInt)(big.NewInt(50)),
			Asset:         "USD",
			InsertionDate: t4,
			EffectiveDate: t4,
			TransactionID: *tx.ID,
		}
		require.NoError(t, store.InsertMoves(ctx, &m5))
		require.NotNil(t, m5.PostCommitEffectiveVolumes)
		require.Equal(t, ledger.Volumes{
			Input:  big.NewInt(150),
			Output: big.NewInt(300),
		}, *m5.PostCommitEffectiveVolumes)
	})

	t.Run("with high concurrency", func(t *testing.T) {
		t.Parallel()

		store := newLedgerStore(t)
		ctx := logging.TestingContext()

		wp := pond.New(10, 10)
		for i := 0; i < 1000; i++ {
			wp.Submit(func() {
				for {
					sqlTx, err := store.GetDB().BeginTx(ctx, &sql.TxOptions{})
					require.NoError(t, err)
					storeCP := store.WithDB(sqlTx)

					src := fmt.Sprintf("accounts:%d", rand.Intn(1000000))
					dst := fmt.Sprintf("accounts:%d", rand.Intn(1000000))

					tx := ledger.NewTransaction().WithPostings(
						ledger.NewPosting(src, dst, "USD", big.NewInt(1)),
					)
					err = storeCP.CommitTransaction(ctx, &tx, nil)
					if errors.Is(err, postgres.ErrDeadlockDetected) {
						require.NoError(t, sqlTx.Rollback())
						continue
					}
					require.NoError(t, err)
					require.NoError(t, sqlTx.Commit())
					return
				}
			})
		}
		wp.StopAndWait()

		aggregatedVolumes, err := store.AggregatedVolumes().GetOne(ctx, ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
			Opts: ledgercontroller.GetAggregatedVolumesOptions{
				UseInsertionDate: true,
			},
		})
		require.NoError(t, err)
		RequireEqual(t, ledger.AggregatedVolumes{
			Aggregated: ledger.VolumesByAssets{
				"USD": {
					Input:  big.NewInt(1000),
					Output: big.NewInt(1000),
				},
			},
		}, *aggregatedVolumes)
	})
}
