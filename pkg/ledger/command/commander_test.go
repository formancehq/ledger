package command

import (
	"context"
	"math/big"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	storageerrors "github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type mockStore struct {
	logs []*core.ChainedLog
}

func (m *mockStore) GetLastLog(ctx context.Context) (*core.ChainedLog, error) {
	if len(m.logs) == 0 {
		return nil, nil
	}
	return m.logs[len(m.logs)-1], nil
}

func (m *mockStore) GetBalanceFromLogs(ctx context.Context, address, asset string) (*big.Int, error) {
	balance := new(big.Int)
	for _, log := range m.logs {
		switch payload := log.Data.(type) {
		case core.NewTransactionLogPayload:
			postings := payload.Transaction.Postings
			for _, posting := range postings {
				if posting.Asset != asset {
					continue
				}
				if posting.Source == address {
					balance = balance.Sub(balance, posting.Amount)
				}
				if posting.Destination == address {
					balance = balance.Add(balance, posting.Amount)
				}
			}
		}
	}
	return balance, nil
}

func (m *mockStore) GetMetadataFromLogs(ctx context.Context, address, key string) (string, error) {
	for i := len(m.logs) - 1; i >= 0; i-- {
		switch payload := m.logs[i].Data.(type) {
		case core.NewTransactionLogPayload:
			forAccount, ok := payload.AccountMetadata[address]
			if ok {
				value, ok := forAccount[key]
				if ok {
					return value, nil
				}
			}
		case core.SetMetadataLogPayload:
			if payload.TargetID != address {
				continue
			}
			value, ok := payload.Metadata[key]
			if ok {
				return value, nil
			}
		}
	}
	return "", errors.New("not found")
}

func (m *mockStore) ReadLogWithIdempotencyKey(ctx context.Context, key string) (*core.ChainedLog, error) {
	first := collectionutils.First(m.logs, func(log *core.ChainedLog) bool {
		return log.IdempotencyKey == key
	})
	if first == nil {
		return nil, storageerrors.ErrNotFound
	}
	return first, nil
}

func (m *mockStore) ReadLogForCreatedTransactionWithReference(ctx context.Context, reference string) (*core.ChainedLog, error) {
	first := collectionutils.First(m.logs, func(log *core.ChainedLog) bool {
		if log.Type != core.NewTransactionLogType {
			return false
		}
		return log.Data.(core.NewTransactionLogPayload).Transaction.Reference == reference
	})
	if first == nil {
		return nil, storageerrors.ErrNotFound
	}
	return first, nil
}

func (m *mockStore) ReadLogForCreatedTransaction(ctx context.Context, txID uint64) (*core.ChainedLog, error) {
	first := collectionutils.First(m.logs, func(log *core.ChainedLog) bool {
		if log.Type != core.NewTransactionLogType {
			return false
		}
		return log.Data.(core.NewTransactionLogPayload).Transaction.ID == txID
	})
	if first == nil {
		return nil, storageerrors.ErrNotFound
	}
	return first, nil
}

func (m *mockStore) ReadLogForRevertedTransaction(ctx context.Context, txID uint64) (*core.ChainedLog, error) {
	first := collectionutils.First(m.logs, func(log *core.ChainedLog) bool {
		if log.Type != core.RevertedTransactionLogType {
			return false
		}
		return log.Data.(core.RevertedTransactionLogPayload).RevertedTransactionID == txID
	})
	if first == nil {
		return nil, storageerrors.ErrNotFound
	}
	return first, nil
}

func (m *mockStore) ReadLastLogWithType(background context.Context, logType ...core.LogType) (*core.ChainedLog, error) {
	first := collectionutils.First(m.logs, func(log *core.ChainedLog) bool {
		return collectionutils.Contains(logType, log.Type)
	})
	if first == nil {
		return nil, storageerrors.ErrNotFound
	}
	return first, nil
}

func (m *mockStore) InsertLogs(ctx context.Context, logs ...*core.ActiveLog) error {

	for _, log := range logs {
		var previousLog *core.ChainedLog
		if len(m.logs) > 0 {
			previousLog = m.logs[len(m.logs)-1]
		}
		chainedLog := log.ChainLog(previousLog)
		m.logs = append(m.logs, chainedLog)
	}

	return nil
}

var (
	_   Store = (*mockStore)(nil)
	now       = core.Now()
)

func newMockStore() *mockStore {
	return &mockStore{
		logs: []*core.ChainedLog{},
	}
}

type testCase struct {
	name          string
	setup         func(t *testing.T, r Store)
	script        string
	reference     string
	expectedError error
	expectedTx    *core.Transaction
	expectedLogs  []*core.Log
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
		expectedTx: core.NewTransaction().WithPostings(
			core.NewPosting("world", "mint", "GEM", big.NewInt(100)),
		),
		expectedLogs: []*core.Log{
			core.NewTransactionLog(
				core.NewTransaction().WithPostings(
					core.NewPosting("world", "mint", "GEM", big.NewInt(100))),
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
			tx := core.NewTransaction().
				WithPostings(core.NewPosting("world", "mint", "GEM", big.NewInt(100))).
				WithReference("tx_ref")
			log := core.NewTransactionLog(tx, nil)
			err := store.InsertLogs(context.Background(), core.NewActiveLog(log.ChainLog(nil)))
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
		expectedTx: core.NewTransaction().
			WithPostings(
				core.NewPosting("world", "mint", "GEM", big.NewInt(100)),
			).
			WithReference("tx_ref"),
		expectedLogs: []*core.Log{
			core.NewTransactionLog(
				core.NewTransaction().
					WithPostings(
						core.NewPosting("world", "mint", "GEM", big.NewInt(100)),
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
		expectedTx: core.NewTransaction().
			WithPostings(
				core.NewPosting("world", "mint", "GEM", big.NewInt(100)),
			),
		expectedLogs: []*core.Log{
			core.NewTransactionLog(
				core.NewTransaction().
					WithPostings(
						core.NewPosting("world", "mint", "GEM", big.NewInt(100)),
					),
				map[string]metadata.Metadata{},
			).WithIdempotencyKey("testing"),
		},
		setup: func(t *testing.T, r Store) {
			log := core.NewTransactionLog(
				core.NewTransaction().
					WithPostings(
						core.NewPosting("world", "mint", "GEM", big.NewInt(100)),
					).
					WithTimestamp(now),
				map[string]metadata.Metadata{},
			).WithIdempotencyKey("testing")
			err := r.InsertLogs(context.Background(), core.NewActiveLog(log.ChainLog(nil)))
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

			store := newMockStore()
			ctx := logging.TestingContext()

			commander := New(store, NoOpLocker, NewCompiler(1024),
				NewReferencer(), nil, func(activeLogs ...*core.ActiveLog) {
					for _, activeLog := range activeLogs {
						activeLog.SetProjected()
					}
				})
			go commander.Run(ctx)
			defer commander.Close()

			if tc.setup != nil {
				tc.setup(t, store)
			}
			ret, err := commander.CreateTransaction(ctx, tc.parameters, core.RunScript{
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
				require.Equal(t, tc.expectedTx, ret)

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
			}
		})
	}
}

func TestRevert(t *testing.T) {
	txID := uint64(0)
	store := newMockStore()
	ctx := logging.TestingContext()

	log := core.NewTransactionLog(
		core.NewTransaction().WithPostings(
			core.NewPosting("world", "bank", "USD", big.NewInt(100)),
		),
		map[string]metadata.Metadata{},
	)
	err := store.InsertLogs(context.Background(), core.NewActiveLog(log.ChainLog(nil)))
	require.NoError(t, err)

	commander := New(store, NoOpLocker, NewCompiler(1024), NewReferencer(), nil, func(activeLogs ...*core.ActiveLog) {
		for _, activeLog := range activeLogs {
			activeLog.SetProjected()
		}
	})
	go commander.Run(ctx)
	defer commander.Close()

	_, err = commander.RevertTransaction(ctx, Parameters{}, txID)
	require.NoError(t, err)
}

func TestRevertWithAlreadyReverted(t *testing.T) {

	store := newMockStore()
	ctx := logging.TestingContext()

	log := core.
		NewRevertedTransactionLog(core.Now(), 0, core.NewTransaction())
	err := store.InsertLogs(context.Background(), core.NewActiveLog(log.ChainLog(nil)))
	require.NoError(t, err)

	commander := New(store, NoOpLocker, NewCompiler(1024), NewReferencer(), nil, func(activeLogs ...*core.ActiveLog) {
		for _, activeLog := range activeLogs {
			activeLog.SetProjected()
		}
	})
	go commander.Run(ctx)
	defer commander.Close()

	_, err = commander.RevertTransaction(context.Background(), Parameters{}, 0)
	require.True(t, errors.Is(err, ErrAlreadyReverted))
}

func TestRevertWithRevertOccurring(t *testing.T) {

	store := newMockStore()
	ctx := logging.TestingContext()

	log := core.NewTransactionLog(
		core.NewTransaction().WithPostings(
			core.NewPosting("world", "bank", "USD", big.NewInt(100)),
		),
		map[string]metadata.Metadata{},
	)
	err := store.InsertLogs(ctx, core.NewActiveLog(log.ChainLog(nil)))
	require.NoError(t, err)

	referencer := NewReferencer()
	commander := New(store, NoOpLocker, NewCompiler(1024),
		referencer, nil, func(activeLogs ...*core.ActiveLog) {
			for _, activeLog := range activeLogs {
				activeLog.SetProjected()
			}
		})
	go commander.Run(ctx)
	defer commander.Close()

	referencer.take(referenceReverts, uint64(0))

	_, err = commander.RevertTransaction(ctx, Parameters{}, 0)
	require.True(t, errors.Is(err, ErrRevertOccurring))
}
