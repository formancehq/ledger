package runner

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/ledger/pkg/machine"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	name             string
	setup            func(t *testing.T, r *Runner)
	script           string
	reference        string
	expectedError    error
	expectedTx       core.ExpandedTransaction
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
		expectedTx: core.ExpandedTransaction{
			Transaction: core.NewTransaction().WithPostings(
				core.NewPosting("world", "mint", "GEM", core.NewMonetaryInt(100)),
			),
			PreCommitVolumes: map[string]core.AssetsVolumes{
				"world": {
					"GEM": core.NewEmptyVolumes(),
				},
				"mint": {
					"GEM": core.NewEmptyVolumes(),
				},
			},
			PostCommitVolumes: map[string]core.AssetsVolumes{
				"world": {
					"GEM": core.NewEmptyVolumes().WithOutput(core.NewMonetaryInt(100)),
				},
				"mint": {
					"GEM": core.NewEmptyVolumes().WithInput(core.NewMonetaryInt(100)),
				},
			},
		},
		expectedLogs: []core.Log{
			core.NewTransactionLog(
				core.NewTransaction().WithPostings(
					core.NewPosting("world", "mint", "GEM", core.NewMonetaryInt(100))),
				map[string]core.Metadata{},
			),
		},
		expectedAccounts: map[string]core.AccountWithVolumes{
			"mint": {
				Account: core.NewAccount("mint"),
				Volumes: core.AssetsVolumes{
					"GEM": core.NewEmptyVolumes().WithInput(core.NewMonetaryInt(100)),
				},
			},
		},
	},
	{
		name:          "no script",
		script:        ``,
		expectedError: machine.NewScriptError(machine.ScriptErrorNoScript, ""),
	},
	{
		name:          "invalid script",
		script:        `XXX`,
		expectedError: machine.NewScriptError(machine.ScriptErrorCompilationFailed, ""),
	},
	{
		name: "set reference conflict",
		setup: func(t *testing.T, l *Runner) {
			tx := core.NewTransaction().
				WithPostings(core.NewPosting("world", "mint", "GEM", core.NewMonetaryInt(100))).
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
		expectedError: NewConflictError(""),
	},
	{
		name: "set reference",
		script: `
			send [GEM 100] (
				source = @world
				destination = @mint
			)`,
		reference: "tx_ref",
		expectedTx: core.ExpandedTransaction{
			Transaction: core.NewTransaction().
				WithPostings(
					core.NewPosting("world", "mint", "GEM", core.NewMonetaryInt(100)),
				).
				WithReference("tx_ref"),
			PreCommitVolumes: map[string]core.AssetsVolumes{
				"world": {
					"GEM": core.NewEmptyVolumes(),
				},
				"mint": {
					"GEM": core.NewEmptyVolumes(),
				},
			},
			PostCommitVolumes: map[string]core.AssetsVolumes{
				"world": {
					"GEM": core.NewEmptyVolumes().WithOutput(core.NewMonetaryInt(100)),
				},
				"mint": {
					"GEM": core.NewEmptyVolumes().WithInput(core.NewMonetaryInt(100)),
				},
			},
		},
		expectedLogs: []core.Log{
			core.NewTransactionLog(
				core.NewTransaction().
					WithPostings(
						core.NewPosting("world", "mint", "GEM", core.NewMonetaryInt(100)),
					).
					WithReference("tx_ref"),
				map[string]core.Metadata{},
			).WithReference("tx_ref"),
		},
		expectedAccounts: map[string]core.AccountWithVolumes{
			"mint": {
				Account: core.NewAccount("mint"),
				Volumes: core.AssetsVolumes{
					"GEM": core.NewEmptyVolumes().WithInput(core.NewMonetaryInt(100)),
				},
			},
		},
	},
}

func TestExecuteScript(t *testing.T) {
	t.Parallel()
	now := core.Now()

	require.NoError(t, pgtesting.CreatePostgresServer())
	defer func() {
		require.NoError(t, pgtesting.DestroyPostgresServer())
	}()

	storageDriver := ledgertesting.StorageDriver(t)
	require.NoError(t, storageDriver.Initialize(context.Background()))

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			ledger := uuid.NewString()

			store, _, err := storageDriver.GetLedgerStore(context.Background(), ledger, true)
			require.NoError(t, err)

			_, err = store.Initialize(context.Background())
			require.NoError(t, err)

			cache := cache.New(store)
			runner, err := New(store, lock.NewInMemory(), cache, false)
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
			}, false, func(transaction core.ExpandedTransaction, accountMetadata map[string]core.Metadata) core.Log {
				return core.NewTransactionLog(transaction.Transaction, accountMetadata)
			})

			if tc.expectedError != nil {
				require.True(t, errors.Is(err, tc.expectedError))
			} else {
				require.NoError(t, err)
				require.NotNil(t, ret)
				tc.expectedTx.Timestamp = now
				require.Equal(t, tc.expectedTx, *ret)

				logs, err := store.ReadLogsStartingFromID(context.Background(), 0)
				require.NoError(t, err)
				require.Len(t, logs, len(tc.expectedLogs))
				for ind := range tc.expectedLogs {
					var previous *core.Log
					if ind > 0 {
						previous = &tc.expectedLogs[ind-1]
					}
					expectedLog := tc.expectedLogs[ind]
					switch v := expectedLog.Data.(type) {
					case core.NewTransactionLogPayload:
						v.Transaction.Timestamp = now
						expectedLog.Data = v
					}
					expectedLog.Date = now
					require.Equal(t, expectedLog.ComputeHash(previous), logs[ind])
				}

				require.Equal(t, tc.expectedTx.Timestamp, runner.lastTransactionDate)

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
