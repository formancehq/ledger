package runner

import (
	"context"
	"math/big"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/numscript"
	"github.com/formancehq/ledger/pkg/ledger/state"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type mockCache struct {
	accounts map[string]*core.AccountWithVolumes
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

func (m *mockCache) UpdateVolumeWithTX(transaction core.Transaction) {
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

type mockStore struct {
	logs []*core.Log
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

type testCase struct {
	name             string
	setup            func(t *testing.T, r *Runner)
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
		setup: func(t *testing.T, l *Runner) {
			tx := core.NewTransaction().
				WithPostings(core.NewPosting("world", "mint", "GEM", big.NewInt(100))).
				WithReference("tx_ref")
			log := core.NewTransactionLog(tx, nil).WithReference("tx_ref")
			require.NoError(t, l.store.AppendLog(
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
		expectedError: state.ErrConflictError,
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

func TestExecuteScript(t *testing.T) {
	t.Parallel()
	now := core.Now()

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			ledger := uuid.NewString()
			cache := &mockCache{
				accounts: map[string]*core.AccountWithVolumes{},
			}
			store := &mockStore{
				logs: []*core.Log{},
			}

			compiler := numscript.NewCompiler()
			locker := lock.New(ledger)
			go func() {
				require.NoError(t, locker.Run(context.Background()))
			}()

			runner, err := New(store, locker, cache, compiler, ledger, false)
			require.NoError(t, err)

			if tc.setup != nil {
				tc.setup(t, runner)
			}
			ret, _, err := runner.Execute(context.Background(), core.RunScript{
				Script: core.Script{
					Plain: tc.script,
				},
				Timestamp: now,
				Reference: tc.reference,
			}, false, func(transaction core.Transaction, accountMetadata map[string]metadata.Metadata) core.Log {
				return core.NewTransactionLog(transaction, accountMetadata)
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

				require.Equal(t, tc.expectedTx.Timestamp, runner.state.GetMoreRecentTransactionDate())

				for address, account := range tc.expectedAccounts {
					accountFromCache, err := runner.cache.GetAccountWithVolumes(context.Background(), address)
					require.NoError(t, err)
					require.NotNil(t, accountFromCache)
					require.Equal(t, account, *accountFromCache)
				}
			}
		})
	}
}
