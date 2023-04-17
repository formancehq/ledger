package command

import (
	"context"
	"math/big"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type mockCache struct {
	accounts map[string]*core.AccountWithVolumes
}

func (m *mockCache) UpdateAccountMetadata(s string, m2 metadata.Metadata) error {
	panic("not implemented")
}

func (m *mockCache) Stop(ctx context.Context) error {
	return nil
}

func (m *mockCache) GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	account, ok := m.accounts[address]
	if !ok {
		account = core.NewAccountWithVolumes(address)
		m.accounts[address] = account
		return account, nil
	}
	return account, nil
}

func (m *mockCache) LockAccounts(ctx context.Context, accounts ...string) (cache.Release, error) {
	return func() {}, nil
}

func (m *mockCache) UpdateVolumeWithTX(transaction *core.Transaction) {
	for _, posting := range transaction.Postings {
		sourceAccount, _ := m.GetAccountWithVolumes(context.Background(), posting.Source)
		sourceAccountAsset := sourceAccount.Volumes[posting.Asset].CopyWithZerosIfNeeded()
		sourceAccountAsset.Output = sourceAccountAsset.Output.Add(sourceAccountAsset.Output, posting.Amount)
		sourceAccount.Volumes[posting.Asset] = sourceAccountAsset
		destAccount, _ := m.GetAccountWithVolumes(context.Background(), posting.Destination)
		destAccountAsset := destAccount.Volumes[posting.Asset].CopyWithZerosIfNeeded()
		destAccountAsset.Input = destAccountAsset.Input.Add(destAccountAsset.Input, posting.Amount)
		destAccount.Volumes[posting.Asset] = destAccountAsset
	}
}

var _ Cache = (*mockCache)(nil)

func newMockCache() *mockCache {
	return &mockCache{
		accounts: map[string]*core.AccountWithVolumes{},
	}
}

type mockStore struct {
	logs         []*core.Log
	transactions map[uint64]*core.ExpandedTransaction
}

func (m *mockStore) Close(ctx context.Context) error {
	return nil
}

func (m *mockStore) GetTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error) {
	tx, ok := m.transactions[id]
	if ok {
		return tx, nil
	}
	return nil, storage.ErrNotFound
}

func (m *mockStore) ReadLastLogWithType(background context.Context, logType ...core.LogType) (*core.Log, error) {
	for _, log := range m.logs {
		for _, logType := range logType {
			if log.Type == logType {
				return log, nil
			}
		}
	}
	return nil, storage.ErrNotFound
}

func (m *mockStore) ReadLogWithReference(ctx context.Context, reference string) (*core.Log, error) {
	for _, log := range m.logs {
		if log.Reference == reference {
			return log, nil
		}
	}
	return nil, storage.ErrNotFound
}

func (m *mockStore) AppendLog(ctx context.Context, log *core.Log) error {
	m.logs = append(m.logs, log)
	return nil
}

var _ Store = (*mockStore)(nil)

func newMockStore() *mockStore {
	return &mockStore{
		logs:         []*core.Log{},
		transactions: map[uint64]*core.ExpandedTransaction{},
	}
}

type testCase struct {
	name             string
	setup            func(t *testing.T, r Store)
	script           string
	reference        string
	expectedError    error
	expectedTx       core.Transaction
	expectedLogs     []core.Log
	expectedAccounts map[string]core.AccountWithVolumes
}

var testCases = []testCase{
	{
		name: "nominal",
		script: `
			send [GEM 100] (
				source = @world
				destination = @mint
			)`,
		expectedTx: core.NewTransaction().WithPostings(
			core.NewPosting("world", "mint", "GEM", big.NewInt(100)),
		),
		expectedLogs: []core.Log{
			core.NewTransactionLog(
				core.NewTransaction().WithPostings(
					core.NewPosting("world", "mint", "GEM", big.NewInt(100))),
				map[string]metadata.Metadata{},
			),
		},
		expectedAccounts: map[string]core.AccountWithVolumes{
			"mint": {
				Account: core.NewAccount("mint"),
				Volumes: core.AssetsVolumes{
					"GEM": core.NewEmptyVolumes().WithInput(big.NewInt(100)),
				},
			},
		},
	},
	{
		name:          "no script",
		script:        ``,
		expectedError: ErrNoScript,
	},
	{
		name:          "invalid script",
		script:        `XXX`,
		expectedError: ErrCompilationFailed,
	},
	{
		name: "set reference conflict",
		setup: func(t *testing.T, store Store) {
			tx := core.NewTransaction().
				WithPostings(core.NewPosting("world", "mint", "GEM", big.NewInt(100))).
				WithReference("tx_ref")
			log := core.NewTransactionLog(tx, nil).WithReference("tx_ref")
			require.NoError(t, store.AppendLog(
				context.Background(),
				&log,
			))
		},
		script: `
			send [GEM 100] (
				source = @world
				destination = @mint
			)`,
		reference:     "tx_ref",
		expectedError: ErrConflictError,
	},
	{
		name: "set reference",
		script: `
			send [GEM 100] (
				source = @world
				destination = @mint
			)`,
		reference: "tx_ref",
		expectedTx: core.NewTransaction().
			WithPostings(
				core.NewPosting("world", "mint", "GEM", big.NewInt(100)),
			).
			WithReference("tx_ref"),
		expectedLogs: []core.Log{
			core.NewTransactionLog(
				core.NewTransaction().
					WithPostings(
						core.NewPosting("world", "mint", "GEM", big.NewInt(100)),
					).
					WithReference("tx_ref"),
				map[string]metadata.Metadata{},
			).WithReference("tx_ref"),
		},
		expectedAccounts: map[string]core.AccountWithVolumes{
			"mint": {
				Account: core.NewAccount("mint"),
				Volumes: core.AssetsVolumes{
					"GEM": core.NewEmptyVolumes().WithInput(big.NewInt(100)),
				},
			},
		},
	},
}

func TestCreateTransaction(t *testing.T) {
	t.Parallel()
	now := core.Now()

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			store := newMockStore()
			cache := newMockCache()

			ledger := New(store, cache, NoOpLocker, NoOpIngester, Load(store, false), nil)

			if tc.setup != nil {
				tc.setup(t, store)
			}
			ret, err := ledger.CreateTransaction(context.Background(), Parameters{}, core.RunScript{
				Script: core.Script{
					Plain: tc.script,
				},
				Timestamp: now,
				Reference: tc.reference,
			})

			if tc.expectedError != nil {
				require.True(t, errors.Is(err, tc.expectedError))
			} else {
				require.NoError(t, err)
				require.NotNil(t, ret)
				tc.expectedTx.Timestamp = now
				require.Equal(t, tc.expectedTx, *ret)

				require.Len(t, store.logs, len(tc.expectedLogs))
				for ind := range tc.expectedLogs {
					expectedLog := tc.expectedLogs[ind]
					switch v := expectedLog.Data.(type) {
					case core.NewTransactionLogPayload:
						v.Transaction.Timestamp = now
						expectedLog.Data = v
					}
					expectedLog.Date = now
				}

				require.Equal(t, tc.expectedTx.Timestamp, ledger.state.GetMoreRecentTransactionDate())

				for address, account := range tc.expectedAccounts {
					accountFromCache, err := ledger.cache.GetAccountWithVolumes(context.Background(), address)
					require.NoError(t, err)
					require.NotNil(t, accountFromCache)
					require.Equal(t, account, *accountFromCache)
				}
			}
		})
	}
}

func TestRevert(t *testing.T) {
	txID := uint64(0)
	store := newMockStore()
	tx := core.ExpandTransactionFromEmptyPreCommitVolumes(
		core.NewTransaction().WithPostings(
			core.NewPosting("world", "bank", "USD", big.NewInt(100)),
		),
	)
	store.transactions[txID] = &tx
	cache := newMockCache()
	cache.accounts["bank"] = &core.AccountWithVolumes{
		Account: core.Account{
			Address:  "bank",
			Metadata: metadata.Metadata{},
		},
		Volumes: map[string]core.Volumes{
			"USD": {
				Input:  big.NewInt(100),
				Output: big.NewInt(0),
			},
		},
	}

	ledger := New(store, cache, NoOpLocker, NoOpIngester, Load(store, false), nil)
	_, err := ledger.RevertTransaction(context.Background(), txID, false)
	require.NoError(t, err)
}

func TestRevertWithAlreadyReverted(t *testing.T) {

	store := newMockStore()
	cache := newMockCache()
	tx := core.ExpandTransactionFromEmptyPreCommitVolumes(core.NewTransaction().WithMetadata(
		core.RevertedMetadata(uint64(0)),
	))
	store.transactions[uint64(0)] = &tx
	cache.accounts["bank"] = &core.AccountWithVolumes{
		Account: core.Account{
			Address:  "bank",
			Metadata: metadata.Metadata{},
		},
		Volumes: map[string]core.Volumes{
			"USD": {
				Input:  big.NewInt(100),
				Output: big.NewInt(0),
			},
		},
	}

	ledger := New(store, cache, NoOpLocker, NoOpIngester, Load(store, false), nil)

	_, err := ledger.RevertTransaction(context.Background(), tx.ID, false)
	require.True(t, errors.Is(err, ErrAlreadyReverted))
}

func TestRevertWithRevertOccurring(t *testing.T) {

	store := newMockStore()
	cache := newMockCache()
	tx := core.ExpandTransactionFromEmptyPreCommitVolumes(
		core.NewTransaction().WithPostings(
			core.NewPosting("world", "bank", "USD", big.NewInt(100)),
		),
	)
	store.transactions[uint64(0)] = &tx
	cache.accounts["bank"] = &core.AccountWithVolumes{
		Account: core.Account{
			Address:  "bank",
			Metadata: metadata.Metadata{},
		},
		Volumes: map[string]core.Volumes{
			"USD": {
				Input:  big.NewInt(100),
				Output: big.NewInt(0),
			},
		},
	}

	ingestedLog := make(chan *core.LogHolder, 1)

	ledger := New(store, cache, NoOpLocker, LogIngesterFn(func(ctx context.Context, log *core.LogHolder) error {
		ingestedLog <- log
		<-log.Ingested
		return nil
	}), Load(store, false), nil)
	go func() {
		_, err := ledger.RevertTransaction(context.Background(), uint64(0), false)
		require.NoError(t, err)

	}()

	log := <-ingestedLog
	defer func() {
		log.SetIngested()
	}()

	_, err := ledger.RevertTransaction(context.Background(), tx.ID, false)
	require.True(t, errors.Is(err, ErrRevertOccurring))
}
