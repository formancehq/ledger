package query

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

type mockStore struct {
	nextLogID    uint64
	logs         []*core.PersistedLog
	accounts     map[string]*core.AccountWithVolumes
	transactions []*core.ExpandedTransaction
}

func (m *mockStore) UpdateAccountsMetadata(ctx context.Context, update []core.Account) error {
	for _, account := range update {
		persistedAccount, ok := m.accounts[account.Address]
		if !ok {
			m.accounts[account.Address] = &core.AccountWithVolumes{
				Account: account,
				Volumes: map[string]core.Volumes{},
			}
			return nil
		}
		persistedAccount.Metadata = persistedAccount.Metadata.Merge(account.Metadata)
	}
	return nil
}

func (m *mockStore) InsertTransactions(ctx context.Context, insert ...core.ExpandedTransaction) error {
	for _, transaction := range insert {
		m.transactions = append(m.transactions, &transaction)
	}
	return nil
}

func (m *mockStore) UpdateTransactionsMetadata(ctx context.Context, update ...core.TransactionWithMetadata) error {
	for _, tx := range update {
		m.transactions[tx.ID].Metadata = m.transactions[tx.ID].Metadata.Merge(tx.Metadata)
	}
	return nil
}

func (m *mockStore) EnsureAccountsExist(ctx context.Context, accounts []string) error {
	for _, address := range accounts {
		_, ok := m.accounts[address]
		if ok {
			continue
		}
		m.accounts[address] = &core.AccountWithVolumes{
			Account: core.Account{
				Address:  address,
				Metadata: metadata.Metadata{},
			},
			Volumes: map[string]core.Volumes{},
		}
	}
	return nil
}

func (m *mockStore) UpdateVolumes(ctx context.Context, updates ...core.AccountsAssetsVolumes) error {
	for _, update := range updates {
		for address, volumes := range update {
			for asset, assetsVolumes := range volumes {
				m.accounts[address].Volumes[asset] = assetsVolumes
			}
		}
	}
	return nil
}

func (m *mockStore) UpdateNextLogID(ctx context.Context, id uint64) error {
	m.nextLogID = id
	return nil
}

func (m *mockStore) IsInitialized() bool {
	return true
}

func (m *mockStore) GetNextLogID(ctx context.Context) (uint64, error) {
	return m.nextLogID, nil
}

func (m *mockStore) ReadLogsRange(ctx context.Context, idMin, idMax uint64) ([]core.PersistedLog, error) {
	if idMax > uint64(len(m.logs)) {
		idMax = uint64(len(m.logs))
	}

	if idMin < uint64(len(m.logs)) {
		return collectionutils.Map(m.logs[idMin:idMax], func(from *core.PersistedLog) core.PersistedLog {
			return *from
		}), nil
	}

	return []core.PersistedLog{}, nil
}

func (m *mockStore) RunInTransaction(ctx context.Context, f func(ctx context.Context, tx Store) error) error {
	return f(ctx, m)
}

func (m *mockStore) GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	account, ok := m.accounts[address]
	if !ok {
		return &core.AccountWithVolumes{
			Account: core.Account{
				Address:  address,
				Metadata: metadata.Metadata{},
			},
			Volumes: map[string]core.Volumes{},
		}, nil
	}
	return account, nil
}

func (m *mockStore) GetTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error) {
	return m.transactions[id], nil
}

var _ Store = (*mockStore)(nil)

func TestWorker(t *testing.T) {
	t.Parallel()

	ledgerStore := &mockStore{
		accounts: map[string]*core.AccountWithVolumes{},
	}

	worker := NewWorker(WorkerConfig{
		ChanSize: 1024,
	}, ledgerStore, "default", monitor.NewNoOpMonitor(), metrics.NewNoOpMetricsRegistry())
	go func() {
		require.NoError(t, worker.Run(context.Background()))
	}()
	defer func() {
		require.NoError(t, worker.Stop(context.Background()))
	}()
	<-worker.Ready()

	var (
		now = core.Now()
	)

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

	logs := []*core.PersistedLog{
		core.NewTransactionLog(tx0, nil).ComputePersistentLog(nil),
		core.NewTransactionLog(tx1, nil).ComputePersistentLog(nil),
		core.NewSetMetadataLog(now, core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeTransaction,
			TargetID:   tx1.ID,
			Metadata:   appliedMetadataOnTX1,
		}).ComputePersistentLog(nil),
		core.NewSetMetadataLog(now, core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeAccount,
			TargetID:   "bank",
			Metadata:   appliedMetadataOnAccount,
		}).ComputePersistentLog(nil),
		core.NewSetMetadataLog(now, core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeAccount,
			TargetID:   "another:account",
			Metadata:   appliedMetadataOnAccount,
		}).ComputePersistentLog(nil),
	}
	for i, persistedLog := range logs {
		persistedLog.ID = uint64(i)
		activeLog := core.NewActiveLog(&persistedLog.Log)
		require.NoError(t, worker.QueueLog(context.Background(), &ledgerstore.AppendedLog{
			ActiveLog:    activeLog,
			PersistedLog: persistedLog,
		}))
		<-activeLog.Ingested
	}
	require.Eventually(t, func() bool {
		nextLogID, err := ledgerStore.GetNextLogID(context.Background())
		require.NoError(t, err)
		return nextLogID == uint64(len(logs))
	}, time.Second, 100*time.Millisecond)

	require.EqualValues(t, 2, len(ledgerStore.transactions))
	require.EqualValues(t, 4, len(ledgerStore.accounts))

	account := ledgerStore.accounts["bank"]
	require.NotNil(t, account)
	require.NotEmpty(t, account.Volumes)
	require.EqualValues(t, 100, account.Volumes["USD/2"].Input.Uint64())
	require.EqualValues(t, 10, account.Volumes["USD/2"].Output.Uint64())

	tx1FromDatabase := ledgerStore.transactions[1]
	tx1.Metadata = appliedMetadataOnTX1
	require.Equal(t, core.ExpandedTransaction{
		Transaction: *tx1,
		PreCommitVolumes: map[string]core.AssetsVolumes{
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
		PostCommitVolumes: map[string]core.AssetsVolumes{
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

	accountWithVolumes := ledgerStore.accounts["bank"]
	require.Equal(t, &core.AccountWithVolumes{
		Account: core.Account{
			Address:  "bank",
			Metadata: appliedMetadataOnAccount,
		},
		Volumes: map[string]core.Volumes{
			"USD/2": {
				Input:  big.NewInt(100),
				Output: big.NewInt(10),
			},
		},
	}, accountWithVolumes)

	accountWithVolumes = ledgerStore.accounts["another:account"]
	require.Equal(t, &core.AccountWithVolumes{
		Account: core.Account{
			Address:  "another:account",
			Metadata: appliedMetadataOnAccount,
		},
		Volumes: map[string]core.Volumes{},
	}, accountWithVolumes)
}
