package query

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/alitto/pond"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

func TestProjector(t *testing.T) {
	t.Parallel()

	ledgerStore := storage.NewInMemoryStore()

	ctx := logging.TestingContext()

	projector := NewProjector(ledgerStore, NewNoOpMonitor(), metrics.NewNoOpRegistry())
	projector.Start(ctx)
	defer projector.Stop(ctx)

	now := core.Now()

	tx0 := core.NewTransaction().WithPostings(
		core.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
	)
	tx1 := core.NewTransaction().WithPostings(
		core.NewPosting("bank", "user:1", "USD/2", big.NewInt(10)),
	).WithID(1)

	appliedMetadataOnTX1 := metadata.Metadata{
		"paymentID": "1234",
	}
	appliedMetadataOnAccount := metadata.Metadata{
		"category": "gold",
	}

	logs := []*core.ChainedLog{
		core.NewTransactionLog(tx0, nil).ChainLog(nil),
		core.NewTransactionLog(tx1, nil).ChainLog(nil),
		core.NewSetMetadataLog(now, core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeTransaction,
			TargetID:   tx1.ID,
			Metadata:   appliedMetadataOnTX1,
		}).ChainLog(nil),
		core.NewSetMetadataLog(now, core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeAccount,
			TargetID:   "bank",
			Metadata:   appliedMetadataOnAccount,
		}).ChainLog(nil),
		core.NewSetMetadataLog(now, core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeAccount,
			TargetID:   "another:account",
			Metadata:   appliedMetadataOnAccount,
		}).ChainLog(nil),
	}
	for i, chainedLog := range logs {
		chainedLog.ID = uint64(i)
		activeLog := core.NewActiveLog(chainedLog)
		projector.QueueLog(activeLog)
		<-activeLog.Projected
	}

	ledgerStore.Logs = logs
	require.Eventually(t, func() bool {
		nextLogID, err := ledgerStore.GetNextLogID(context.Background())
		require.NoError(t, err)
		return nextLogID == uint64(len(logs))
	}, time.Second, 100*time.Millisecond)

	require.EqualValues(t, 2, len(ledgerStore.Transactions))
	require.EqualValues(t, 4, len(ledgerStore.Accounts))

	account := ledgerStore.Accounts["bank"]
	require.NotNil(t, account)
	require.NotEmpty(t, account.Volumes)
	require.EqualValues(t, 100, account.Volumes["USD/2"].Input.Uint64())
	require.EqualValues(t, 10, account.Volumes["USD/2"].Output.Uint64())

	tx1FromDatabase := ledgerStore.Transactions[1]
	tx1.Metadata = appliedMetadataOnTX1
	require.Equal(t, core.ExpandedTransaction{
		Transaction: *tx1,
		PreCommitVolumes: map[string]core.VolumesByAssets{
			"bank": {
				"USD/2": {
					Input:  big.NewInt(100),
					Output: big.NewInt(0),
				},
			},
			"user:1": {
				"USD/2": {
					Output: big.NewInt(0),
					Input:  big.NewInt(0),
				},
			},
		},
		PostCommitVolumes: map[string]core.VolumesByAssets{
			"bank": {
				"USD/2": {
					Input:  big.NewInt(100),
					Output: big.NewInt(10),
				},
			},
			"user:1": {
				"USD/2": {
					Input:  big.NewInt(10),
					Output: big.NewInt(0),
				},
			},
		},
	}, *tx1FromDatabase)

	accountWithVolumes := ledgerStore.Accounts["bank"]
	require.Equal(t, &core.AccountWithVolumes{
		Account: core.Account{
			Address:  "bank",
			Metadata: appliedMetadataOnAccount,
		},
		Volumes: core.VolumesByAssets{
			"USD/2": {
				Input:  big.NewInt(100),
				Output: big.NewInt(10),
			},
		},
	}, accountWithVolumes)

	accountWithVolumes = ledgerStore.Accounts["another:account"]
	require.Equal(t, &core.AccountWithVolumes{
		Account: core.Account{
			Address:  "another:account",
			Metadata: appliedMetadataOnAccount,
		},
		Volumes: core.VolumesByAssets{},
	}, accountWithVolumes)
}

func TestProjectorUnderHeavyParallelLoad(t *testing.T) {
	t.Parallel()

	const nbWorkers = 5
	pool := pond.New(nbWorkers, nbWorkers)
	ledgerStore := storage.NewInMemoryStore()

	ctx := logging.ContextWithLogger(context.TODO(), logging.Testing())

	projector := NewProjector(ledgerStore, NewNoOpMonitor(), metrics.NewNoOpRegistry())
	projector.Start(ctx)
	defer projector.Stop(ctx)

	var (
		previousLog *core.ChainedLog
		allLogs     = make([]*core.ActiveLog, 0)
	)
	for i := 0; i < nbWorkers*500; i++ {
		log := core.NewTransactionLog(core.NewTransaction().WithID(uint64(i)).WithPostings(
			core.NewPosting("world", fmt.Sprintf("accounts:%d", i%100), "USD/2", big.NewInt(100)),
		), nil).ChainLog(previousLog)
		activeLog := core.NewActiveLog(log)
		pool.Submit(func() {
			projector.QueueLog(activeLog)
		})
		previousLog = log
		allLogs = append(allLogs, activeLog)
	}

	pool.StopAndWait()
	for _, log := range allLogs {
		select {
		case <-log.Projected:
		case <-time.After(time.Second):
			require.Fail(t, fmt.Sprintf("log %d must have been ingested", log.ID))
		}
	}
}
