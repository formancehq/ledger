//go:build it

package ledger_test

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v3/logging"
	ledger "github.com/formancehq/ledger/internal"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/internal/storage/workers/lockmonitor"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"go.uber.org/mock/gomock"
	"math/big"
	"sync"
	"testing"
	"time"
)

func TestAccountsVolumesLocksMonitor(t *testing.T) {
	// todo: need to refine the lock management to parallelize
	//t.Parallel()

	ctx := logging.TestingContext()
	ledgerStore := newLedgerStore(t)
	ctrl := gomock.NewController(t)

	unlocked := make(chan struct{})
	// lock the accounts_volumes table to control locks taken by the store
	go func() {
		err := ledgerStore.GetDB().RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			_, err := tx.NewRaw(`LOCK TABLE "_default".accounts_volumes`).
				Exec(ctx)
			if err != nil {
				return err
			}

			<-unlocked

			return nil
		})
		require.NoError(t, err)
	}()

	const countTx = 50

	// simulate a bunch of transactions that will try to update the accounts_volumes table
	wg := sync.WaitGroup{}
	for i := 0; i < countTx; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := ledgerStore.UpdateVolumes(ctx,
				ledger.AccountsVolumes{
					Account: "world",
					Asset:   "USD/2",
					Input:   big.NewInt(0),
					Output:  big.NewInt(100),
				},
				ledger.AccountsVolumes{
					Account: fmt.Sprintf("bank:%d", i%5),
					Asset:   "USD/2",
					Input:   big.NewInt(50),
					Output:  big.NewInt(0),
				},
				ledger.AccountsVolumes{
					Account: uuid.NewString(),
					Asset:   "USD/2",
					Input:   big.NewInt(100),
					Output:  big.NewInt(0),
				},
			)
			require.NoError(t, err)
		}()
	}

	t.Cleanup(func() {
		close(unlocked)
		wg.Wait()
	})

	// wait for all transactions to be started and locks to be taken
	require.Eventually(t, func() bool {
		lockCount, err := ledgerStore.GetDB().
			NewSelect().
			Table("pg_locks").
			Where("not granted").
			Count(ctx)
		require.NoError(t, err)

		return lockCount == countTx
	}, 5*time.Second, 100*time.Millisecond)

	recorder := NewMockRecorder(ctrl)
	w := lockmonitor.NewWorker(logging.Testing(), ledgerStore.GetDB(), lockmonitor.Config{
		Interval: time.Second,
	}, lockmonitor.WithMonitors(
		ledgerstore.NewAccountsVolumesMonitor(recorder),
	))

	recorder.EXPECT().Record(gomock.Any(), map[string]map[string]map[string]int{
		ledgerStore.GetLedger().Name: {
			"bank:0": {
				"USD/2": 45,
			},
			"bank:1": {
				"USD/2": 45,
			},
			"bank:2": {
				"USD/2": 45,
			},
			"bank:3": {
				"USD/2": 45,
			},
			"bank:4": {
				"USD/2": 45,
			},
			"world": {
				"USD/2": 1225,
			},
		},
	})

	err := w.Fire(ctx)
	require.NoError(t, err)
}
