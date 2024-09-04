//go:build it

package ledger

import (
	"database/sql"
	"fmt"
	"github.com/alitto/pond"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"math/big"
	"math/rand"
	"testing"
)

//func TestMoves(t *testing.T) {
//	t.Parallel()
//
//	store := newLedgerStore(t)
//	ctx := logging.TestingContext()
//
//	now := time.Now()
//	_, err := store.upsertAccount(ctx, ledger.Account{
//		BaseModel:     bun.BaseModel{},
//		Address:       "world",
//		Metadata:      metadata.Metadata{},
//		FirstUsage:    now,
//		InsertionDate: now,
//		UpdatedAt:     now,
//	})
//	require.NoError(t, err)
//
//	_, err = store.upsertAccount(ctx, ledger.Account{
//		BaseModel:     bun.BaseModel{},
//		Address:       "bank",
//		Metadata:      metadata.Metadata{},
//		FirstUsage:    now,
//		InsertionDate: now,
//		UpdatedAt:     now,
//	})
//	require.NoError(t, err)
//
//	_, err = store.upsertAccount(ctx, ledger.Account{
//		BaseModel:     bun.BaseModel{},
//		Address:       "bank2",
//		Metadata:      metadata.Metadata{},
//		FirstUsage:    now,
//		InsertionDate: now,
//		UpdatedAt:     now,
//	})
//	require.NoError(t, err)
//
//	// Insert first tx
//	tx1, err := store.CommitTransaction(ctx, ledger.NewTransactionData().WithPostings(
//		ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
//	).WithTimestamp(now))
//	require.NoError(t, err)
//
//	for _, move := range tx1.GetMoves() {
//		require.NoError(t, store.insertMoves(ctx, move))
//	}
//
//	balance, err := store.GetBalance(ctx, "world", "USD/2")
//	require.NoError(t, err)
//	require.Equal(t, big.NewInt(-100), balance)
//
//	balance, err = store.GetBalance(ctx, "bank", "USD/2")
//	require.NoError(t, err)
//	require.Equal(t, big.NewInt(100), balance)
//
//	// Insert second tx
//	tx2, err := store.CommitTransaction(ctx, ledger.NewTransactionData().WithPostings(
//		ledger.NewPosting("world", "bank2", "USD/2", big.NewInt(100)),
//	).WithTimestamp(now.Add(time.Minute)))
//	require.NoError(t, err)
//
//	for _, move := range tx2.GetMoves() {
//		require.NoError(t, store.insertMoves(ctx, move))
//	}
//
//	balance, err = store.GetBalance(ctx, "world", "USD/2")
//	require.NoError(t, err)
//	require.Equal(t, big.NewInt(-200), balance)
//
//	balance, err = store.GetBalance(ctx, "bank2", "USD/2")
//	require.NoError(t, err)
//	require.Equal(t, big.NewInt(100), balance)
//
//	// Insert backdated tx
//	tx3, err := store.CommitTransaction(ctx, ledger.NewTransactionData().WithPostings(
//		ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
//	).WithTimestamp(now.Add(30*time.Second)))
//	require.NoError(t, err)
//
//	for _, move := range tx3.GetMoves() {
//		require.NoError(t, store.insertMoves(ctx, move))
//	}
//
//	balance, err = store.GetBalance(ctx, "world", "USD/2")
//	require.NoError(t, err)
//	require.Equal(t, big.NewInt(-300), balance)
//
//	balance, err = store.GetBalance(ctx, "bank", "USD/2")
//	require.NoError(t, err)
//	require.Equal(t, big.NewInt(200), balance)
//}

func TestPostCommitVolumesComputation(t *testing.T) {
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
				err = storeCP.CommitTransaction(ctx, &tx)
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

	aggregatedBalances, err := store.GetAggregatedBalances(ctx, ledgercontroller.NewGetAggregatedBalancesQuery(ledgercontroller.PITFilter{}, nil, true))
	require.NoError(t, err)
	RequireEqual(t, ledger.BalancesByAssets{
		"USD": big.NewInt(0),
	}, aggregatedBalances)
}
