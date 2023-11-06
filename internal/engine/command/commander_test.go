package command

import (
	"context"
	"math/big"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/bus"
	storageerrors "github.com/formancehq/ledger/internal/storage"
	internaltesting "github.com/formancehq/ledger/internal/testing"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var (
	now = ledger.Now()
)

type testCase struct {
	name          string
	setup         func(t *testing.T, r Store)
	script        string
	reference     string
	expectedError error
	expectedTx    *ledger.Transaction
	expectedLogs  []*ledger.Log
	parameters    Parameters
}

var testCases = []testCase{
	{
		name: "nominal",
		script: `
			send [GEM 100] (
				source = @world
				destination = @mint
			)`,
		expectedTx: ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", "mint", "GEM", big.NewInt(100)),
		),
		expectedLogs: []*ledger.Log{
			ledger.NewTransactionLog(
				ledger.NewTransaction().WithPostings(
					ledger.NewPosting("world", "mint", "GEM", big.NewInt(100))),
				map[string]metadata.Metadata{},
			),
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
			tx := ledger.NewTransaction().
				WithPostings(ledger.NewPosting("world", "mint", "GEM", big.NewInt(100))).
				WithReference("tx_ref")
			log := ledger.NewTransactionLog(tx, nil)
			err := store.InsertLogs(context.Background(), log.ChainLog(nil))
			require.NoError(t, err)
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
		expectedTx: ledger.NewTransaction().
			WithPostings(
				ledger.NewPosting("world", "mint", "GEM", big.NewInt(100)),
			).
			WithReference("tx_ref"),
		expectedLogs: []*ledger.Log{
			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(
						ledger.NewPosting("world", "mint", "GEM", big.NewInt(100)),
					).
					WithReference("tx_ref"),
				map[string]metadata.Metadata{},
			),
		},
	},
	{
		name: "using idempotency",
		script: `
			send [GEM 100] (
				source = @world
				destination = @mint
			)`,
		reference: "tx_ref",
		expectedTx: ledger.NewTransaction().
			WithPostings(
				ledger.NewPosting("world", "mint", "GEM", big.NewInt(100)),
			),
		expectedLogs: []*ledger.Log{
			ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(
						ledger.NewPosting("world", "mint", "GEM", big.NewInt(100)),
					),
				map[string]metadata.Metadata{},
			).WithIdempotencyKey("testing"),
		},
		setup: func(t *testing.T, r Store) {
			log := ledger.NewTransactionLog(
				ledger.NewTransaction().
					WithPostings(
						ledger.NewPosting("world", "mint", "GEM", big.NewInt(100)),
					).
					WithDate(now),
				map[string]metadata.Metadata{},
			).WithIdempotencyKey("testing")
			err := r.InsertLogs(context.Background(), log.ChainLog(nil))
			require.NoError(t, err)
		},
		parameters: Parameters{
			IdempotencyKey: "testing",
		},
	},
}

func TestCreateTransaction(t *testing.T) {
	t.Parallel()

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			store := storageerrors.NewInMemoryStore()
			ctx := logging.TestingContext()

			commander := New(store, NoOpLocker, NewCompiler(1024), NewReferencer(), bus.NewNoOpMonitor())
			go commander.Run(ctx)
			defer commander.Close()

			if tc.setup != nil {
				tc.setup(t, store)
			}
			ret, err := commander.CreateTransaction(ctx, tc.parameters, ledger.RunScript{
				Script: ledger.Script{
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
				internaltesting.RequireEqual(t, tc.expectedTx, ret)

				for ind := range tc.expectedLogs {
					expectedLog := tc.expectedLogs[ind]
					switch v := expectedLog.Data.(type) {
					case ledger.NewTransactionLogPayload:
						v.Transaction.Timestamp = now
						expectedLog.Data = v
					}
					expectedLog.Date = now
				}
			}
		})
	}
}

func TestRevert(t *testing.T) {
	txID := big.NewInt(0)
	store := storageerrors.NewInMemoryStore()
	ctx := logging.TestingContext()

	log := ledger.NewTransactionLog(
		ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
		),
		map[string]metadata.Metadata{},
	).ChainLog(nil)
	err := store.InsertLogs(context.Background(), log)
	require.NoError(t, err)

	commander := New(store, NoOpLocker, NewCompiler(1024), NewReferencer(), bus.NewNoOpMonitor())
	go commander.Run(ctx)
	defer commander.Close()

	_, err = commander.RevertTransaction(ctx, Parameters{}, txID, false)
	require.NoError(t, err)
}

func TestRevertWithAlreadyReverted(t *testing.T) {

	store := storageerrors.NewInMemoryStore()
	ctx := logging.TestingContext()

	tx := ledger.NewTransaction().WithPostings(ledger.NewPosting("world", "bank", "USD", big.NewInt(100)))
	err := store.InsertLogs(context.Background(),
		ledger.NewTransactionLog(tx, map[string]metadata.Metadata{}).ChainLog(nil),
		ledger.NewRevertedTransactionLog(ledger.Now(), tx.ID, ledger.NewTransaction()).ChainLog(nil),
	)
	require.NoError(t, err)

	commander := New(store, NoOpLocker, NewCompiler(1024), NewReferencer(), bus.NewNoOpMonitor())
	go commander.Run(ctx)
	defer commander.Close()

	_, err = commander.RevertTransaction(context.Background(), Parameters{}, tx.ID, false)
	require.True(t, errors.Is(err, ErrAlreadyReverted))
}

func TestRevertWithRevertOccurring(t *testing.T) {

	store := storageerrors.NewInMemoryStore()
	ctx := logging.TestingContext()

	tx := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
	)
	log := ledger.NewTransactionLog(tx, map[string]metadata.Metadata{})
	err := store.InsertLogs(ctx, log.ChainLog(nil))
	require.NoError(t, err)

	referencer := NewReferencer()
	commander := New(store, NoOpLocker, NewCompiler(1024), referencer, bus.NewNoOpMonitor())
	go commander.Run(ctx)
	defer commander.Close()

	referencer.take(referenceReverts, big.NewInt(0))

	_, err = commander.RevertTransaction(ctx, Parameters{}, tx.ID, false)
	require.True(t, errors.Is(err, ErrRevertOccurring))
}

func TestForceRevert(t *testing.T) {

	store := storageerrors.NewInMemoryStore()
	ctx := logging.TestingContext()

	tx1 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
	)
	tx2 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("bank", "foo", "USD", big.NewInt(100)),
	)
	err := store.InsertLogs(ctx, ledger.ChainLogs(
		ledger.NewTransactionLog(tx1, map[string]metadata.Metadata{}),
		ledger.NewTransactionLog(tx2, map[string]metadata.Metadata{}),
	)...)
	require.NoError(t, err)

	commander := New(store, NoOpLocker, NewCompiler(1024), NewReferencer(), bus.NewNoOpMonitor())
	go commander.Run(ctx)
	defer commander.Close()

	_, err = commander.RevertTransaction(ctx, Parameters{}, tx1.ID, false)
	require.NotNil(t, err)
	require.True(t, errors.Is(err, ledger.ErrInsufficientFund))

	balance, err := store.GetBalance(ctx, "bank", "USD")
	require.NoError(t, err)
	require.Equal(t, uint64(0), balance.Uint64())

	_, err = commander.RevertTransaction(ctx, Parameters{}, tx1.ID, true)
	require.Nil(t, err)

	balance, err = store.GetBalance(ctx, "bank", "USD")
	require.NoError(t, err)
	require.Equal(t, big.NewInt(-100), balance)

	balance, err = store.GetBalance(ctx, "world", "USD")
	require.NoError(t, err)
	require.Equal(t, uint64(0), balance.Uint64())
}
