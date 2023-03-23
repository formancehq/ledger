package query

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestWorker(t *testing.T) {
	t.Parallel()

	require.NoError(t, pgtesting.CreatePostgresServer())
	defer func() {
		require.NoError(t, pgtesting.DestroyPostgresServer())
	}()

	driver := ledgertesting.StorageDriver(t)
	require.NoError(t, driver.Initialize(context.Background()))

	ledgerStore, _, err := driver.GetLedgerStore(context.Background(), uuid.New(), true)
	require.NoError(t, err)

	modified, err := ledgerStore.Initialize(context.Background())
	require.NoError(t, err)
	require.True(t, modified)

	worker := NewWorker(workerConfig{
		ChanSize: 1024,
	}, driver, NewNoOpMonitor())
	go func() {
		require.NoError(t, worker.Run(context.Background()))
	}()
	defer func() {
		require.NoError(t, worker.Stop(context.Background()))
	}()

	var (
		now = core.Now()
	)

	tx0 := core.Transaction{
		TransactionData: core.TransactionData{
			Postings: []core.Posting{{
				Source:      "world",
				Destination: "bank",
				Amount:      core.NewMonetaryInt(100),
				Asset:       "USD/2",
			}},
			Timestamp: now,
		},
		ID: 0,
	}
	tx1 := core.Transaction{
		TransactionData: core.TransactionData{
			Postings: []core.Posting{{
				Source:      "bank",
				Destination: "user:1",
				Amount:      core.NewMonetaryInt(10),
				Asset:       "USD/2",
			}},
			Timestamp: now,
		},
		ID: 1,
	}

	appliedMetadataOnTX1 := core.Metadata{
		"paymentID": "1234",
	}
	appliedMetadataOnAccount := core.Metadata{
		"category": "gold",
	}

	logs := []core.Log{
		core.NewTransactionLog(tx0, nil),
		core.NewTransactionLog(tx1, nil),
		core.NewSetMetadataLog(now, core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeTransaction,
			TargetID:   tx1.ID,
			Metadata:   appliedMetadataOnTX1,
		}),
		core.NewSetMetadataLog(now, core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeAccount,
			TargetID:   "bank",
			Metadata:   appliedMetadataOnAccount,
		}),
		core.NewSetMetadataLog(now, core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeAccount,
			TargetID:   "another:account",
			Metadata:   appliedMetadataOnAccount,
		}),
	}
	for _, log := range logs {
		require.NoError(t, ledgerStore.AppendLog(context.Background(), &log))
		<-worker.QueueLog(context.Background(), log, ledgerStore)
	}
	require.Eventually(t, func() bool {
		nextLogID, err := ledgerStore.GetNextLogID(context.Background())
		require.NoError(t, err)
		return nextLogID == uint64(len(logs))
	}, time.Second, 100*time.Millisecond)

	count, err := ledgerStore.CountTransactions(context.Background(), *storage.NewTransactionsQuery())
	require.NoError(t, err)
	require.EqualValues(t, 2, count)

	count, err = ledgerStore.CountAccounts(context.Background(), *storage.NewAccountsQuery())
	require.NoError(t, err)
	require.EqualValues(t, 4, count)

	account, err := ledgerStore.GetAccountWithVolumes(context.Background(), "bank")
	require.NoError(t, err)
	require.NotEmpty(t, account.Volumes)
	require.EqualValues(t, 100, account.Volumes["USD/2"].Input.Uint64())
	require.EqualValues(t, 10, account.Volumes["USD/2"].Output.Uint64())

	tx1FromDatabase, err := ledgerStore.GetTransaction(context.Background(), 1)
	tx1.Metadata = appliedMetadataOnTX1
	require.NoError(t, err)
	require.Equal(t, core.ExpandedTransaction{
		Transaction: tx1,
		PreCommitVolumes: map[string]core.AssetsVolumes{
			"bank": {
				"USD/2": {
					Input:  core.NewMonetaryInt(100),
					Output: core.NewMonetaryInt(0),
				},
			},
			"user:1": {
				"USD/2": {
					Output: core.NewMonetaryInt(0),
					Input:  core.NewMonetaryInt(0),
				},
			},
		},
		PostCommitVolumes: map[string]core.AssetsVolumes{
			"bank": {
				"USD/2": {
					Input:  core.NewMonetaryInt(100),
					Output: core.NewMonetaryInt(10),
				},
			},
			"user:1": {
				"USD/2": {
					Input:  core.NewMonetaryInt(10),
					Output: core.NewMonetaryInt(0),
				},
			},
		},
	}, *tx1FromDatabase)

	accountWithVolumes, err := ledgerStore.GetAccountWithVolumes(context.Background(), "bank")
	require.NoError(t, err)
	require.Equal(t, &core.AccountWithVolumes{
		Account: core.Account{
			Address:  "bank",
			Metadata: appliedMetadataOnAccount,
		},
		Volumes: map[string]core.Volumes{
			"USD/2": {
				Input:  core.NewMonetaryInt(100),
				Output: core.NewMonetaryInt(10),
			},
		},
	}, accountWithVolumes)

	accountWithVolumes, err = ledgerStore.GetAccountWithVolumes(context.Background(), "another:account")
	require.NoError(t, err)
	require.Equal(t, &core.AccountWithVolumes{
		Account: core.Account{
			Address:  "another:account",
			Metadata: appliedMetadataOnAccount,
		},
		Volumes: map[string]core.Volumes{},
	}, accountWithVolumes)
}
