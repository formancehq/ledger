package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/pointer"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/storage/common"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

// mockPaginatedResource is a test implementation of common.PaginatedResource for ledgers.
type mockPaginatedResource struct {
	paginateFn func(ctx context.Context, query common.PaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error)
}

func (m *mockPaginatedResource) Paginate(ctx context.Context, query common.PaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error) {
	return m.paginateFn(ctx, query)
}

func (m *mockPaginatedResource) GetOne(context.Context, common.ResourceQuery[systemstore.ListLedgersQueryPayload]) (*ledger.Ledger, error) {
	return nil, nil
}

func (m *mockPaginatedResource) Count(context.Context, common.ResourceQuery[systemstore.ListLedgersQueryPayload]) (int, error) {
	return 0, nil
}

// testGlobalExporterStateStore is a test implementation of GlobalExporterStateStore.
type testGlobalExporterStateStore struct {
	ledgersResource common.PaginatedResource[ledger.Ledger, systemstore.ListLedgersQueryPayload]
	states          map[string]uint64
	updateCh        chan updateStateCall
	deleteCalled    chan struct{}
	listErr         error
	updateErr       error
	deleteErr       error
}

type updateStateCall struct {
	ledger    string
	lastLogID uint64
}

func (s *testGlobalExporterStateStore) Ledgers() common.PaginatedResource[ledger.Ledger, systemstore.ListLedgersQueryPayload] {
	return s.ledgersResource
}

func (s *testGlobalExporterStateStore) ListGlobalExporterStates(_ context.Context) (map[string]uint64, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.states, nil
}

func (s *testGlobalExporterStateStore) UpdateGlobalExporterState(_ context.Context, name string, lastLogID uint64) error {
	if s.updateCh != nil {
		s.updateCh <- updateStateCall{ledger: name, lastLogID: lastLogID}
	}
	return s.updateErr
}

func (s *testGlobalExporterStateStore) DeleteAllGlobalExporterStates(_ context.Context) error {
	if s.deleteCalled != nil {
		select {
		case <-s.deleteCalled:
		default:
			close(s.deleteCalled)
		}
	}
	return s.deleteErr
}

func TestGlobalExporterProgressTracker(t *testing.T) {
	t.Parallel()

	t.Run("LedgerName", func(t *testing.T) {
		tracker := &globalExporterProgressTracker{
			ledgerName: "test-ledger",
		}
		require.Equal(t, "test-ledger", tracker.LedgerName())
	})

	t.Run("LastLogID nil initially", func(t *testing.T) {
		tracker := &globalExporterProgressTracker{}
		require.Nil(t, tracker.LastLogID())
	})

	t.Run("LastLogID returns set value", func(t *testing.T) {
		id := uint64(42)
		tracker := &globalExporterProgressTracker{
			lastLogID: &id,
		}
		require.Equal(t, pointer.For(uint64(42)), tracker.LastLogID())
	})

	t.Run("UpdateLastLogID persists and updates in-memory", func(t *testing.T) {
		updateCh := make(chan updateStateCall, 1)
		store := &testGlobalExporterStateStore{updateCh: updateCh}
		tracker := &globalExporterProgressTracker{
			ledgerName: "my-ledger",
			store:      store,
			logger:     logging.Testing(),
		}

		err := tracker.UpdateLastLogID(context.Background(), 99)
		require.NoError(t, err)
		require.Equal(t, pointer.For(uint64(99)), tracker.LastLogID())

		call := <-updateCh
		require.Equal(t, "my-ledger", call.ledger)
		require.Equal(t, uint64(99), call.lastLogID)
	})

	t.Run("UpdateLastLogID swallows store error", func(t *testing.T) {
		store := &testGlobalExporterStateStore{updateErr: errors.New("db down")}
		tracker := &globalExporterProgressTracker{
			ledgerName: "my-ledger",
			store:      store,
			logger:     logging.Testing(),
		}

		// Should not return an error even if store fails
		err := tracker.UpdateLastLogID(context.Background(), 5)
		require.NoError(t, err)
		require.Equal(t, pointer.For(uint64(5)), tracker.LastLogID())
	})
}

func TestNewGlobalExporterRunnerDefaults(t *testing.T) {
	t.Parallel()

	store := &testGlobalExporterStateStore{}
	runner := NewGlobalExporterRunner(
		store,
		nil,
		nil,
		logging.Testing(),
		GlobalExporterRunnerConfig{},
	)

	require.Equal(t, DefaultGlobalExporterPullInterval, runner.config.PullInterval)
	require.Equal(t, DefaultGlobalExporterPushRetryPeriod, runner.config.PushRetryPeriod)
	require.Equal(t, DefaultGlobalExporterLogsPageSize, runner.config.LogsPageSize)
	require.Equal(t, DefaultGlobalExporterLedgerPullInterval, runner.config.LedgerPullInterval)
}

func TestNewGlobalExporterRunnerCustomConfig(t *testing.T) {
	t.Parallel()

	store := &testGlobalExporterStateStore{}
	runner := NewGlobalExporterRunner(
		store,
		nil,
		nil,
		logging.Testing(),
		GlobalExporterRunnerConfig{
			PullInterval:       5 * time.Second,
			PushRetryPeriod:    30 * time.Second,
			LogsPageSize:       50,
			LedgerPullInterval: 20 * time.Second,
		},
	)

	require.Equal(t, 5*time.Second, runner.config.PullInterval)
	require.Equal(t, 30*time.Second, runner.config.PushRetryPeriod)
	require.Equal(t, uint64(50), runner.config.LogsPageSize)
	require.Equal(t, 20*time.Second, runner.config.LedgerPullInterval)
}

func TestNewGlobalExporterRunnerCachesLogFetcher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	fetcher := NewMockLogFetcher(ctrl)
	callCount := 0

	runner := NewGlobalExporterRunner(
		&testGlobalExporterStateStore{},
		nil,
		func(ctx context.Context, name string) (LogFetcher, error) {
			callCount++
			return fetcher, nil
		},
		logging.Testing(),
		GlobalExporterRunnerConfig{},
	)

	ctx := context.Background()
	f1, err := runner.getLogFetcher(ctx, "ledger1")
	require.NoError(t, err)
	f2, err := runner.getLogFetcher(ctx, "ledger1")
	require.NoError(t, err)

	require.Same(t, f1, f2)
	require.Equal(t, 1, callCount) // Only called once due to cache
}

func TestGlobalExporterRunnerNominal(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	driver := drivers.NewMockDriver(ctrl)

	log := ledger.NewLog(ledger.CreatedTransaction{
		Transaction: ledger.NewTransaction(),
	})
	log.ID = pointer.For(uint64(1))

	deliver := make(chan struct{})
	delivered := make(chan struct{})

	logFetcher.EXPECT().
		ListLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, _ common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-deliver:
				select {
				case <-delivered:
				default:
					close(delivered)
					return &bunpaginate.Cursor[ledger.Log]{
						Data: []ledger.Log{log},
					}, nil
				}
			}
			return &bunpaginate.Cursor[ledger.Log]{}, nil
		})

	driver.EXPECT().Start(gomock.Any()).Return(nil)
	driver.EXPECT().
		Accept(gomock.Any(), drivers.NewLogWithLedger("ledger1", log)).
		Return([]error{nil}, nil)

	updateCh := make(chan updateStateCall, 1)

	store := &testGlobalExporterStateStore{
		ledgersResource: &mockPaginatedResource{
			paginateFn: func(ctx context.Context, query common.PaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error) {
				return &bunpaginate.Cursor[ledger.Ledger]{
					Data: []ledger.Ledger{{ID: 1, Name: "ledger1"}},
				}, nil
			},
		},
		states:   map[string]uint64{},
		updateCh: updateCh,
	}

	runner := NewGlobalExporterRunner(
		store,
		driver,
		func(ctx context.Context, name string) (LogFetcher, error) {
			return logFetcher, nil
		},
		logging.Testing(),
		GlobalExporterRunnerConfig{
			PullInterval:       50 * time.Millisecond,
			PushRetryPeriod:    50 * time.Millisecond,
			LogsPageSize:       100,
			LedgerPullInterval: 50 * time.Millisecond,
		},
	)

	go runner.Run(ctx)

	// Let the driver become ready and the runner discover ledgers
	close(deliver)

	// Wait for the log to be exported and state updated
	require.Eventually(t, func() bool {
		select {
		case call := <-updateCh:
			return call.ledger == "ledger1" && call.lastLogID == 1
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond)

	driver.EXPECT().Stop(gomock.Any()).Return(nil)
	require.NoError(t, runner.Shutdown(ctx))

	require.Eventually(t, ctrl.Satisfied, 2*time.Second, 10*time.Millisecond)
}

func TestGlobalExporterRunnerWithExistingState(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	driver := drivers.NewMockDriver(ctrl)

	// Only return log with ID=2 (simulating catch-up from ID=1)
	log := ledger.NewLog(ledger.CreatedTransaction{
		Transaction: ledger.NewTransaction(),
	})
	log.ID = pointer.For(uint64(2))

	deliver := make(chan struct{})
	delivered := make(chan struct{})

	logFetcher.EXPECT().
		ListLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, _ common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-deliver:
				select {
				case <-delivered:
				default:
					close(delivered)
					return &bunpaginate.Cursor[ledger.Log]{
						Data: []ledger.Log{log},
					}, nil
				}
			}
			return &bunpaginate.Cursor[ledger.Log]{}, nil
		})

	driver.EXPECT().Start(gomock.Any()).Return(nil)
	driver.EXPECT().
		Accept(gomock.Any(), drivers.NewLogWithLedger("ledger1", log)).
		Return([]error{nil}, nil)

	updateCh := make(chan updateStateCall, 1)

	store := &testGlobalExporterStateStore{
		ledgersResource: &mockPaginatedResource{
			paginateFn: func(ctx context.Context, query common.PaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error) {
				return &bunpaginate.Cursor[ledger.Ledger]{
					Data: []ledger.Ledger{{ID: 1, Name: "ledger1"}},
				}, nil
			},
		},
		states:   map[string]uint64{"ledger1": 1}, // Already processed log 1
		updateCh: updateCh,
	}

	runner := NewGlobalExporterRunner(
		store,
		driver,
		func(ctx context.Context, name string) (LogFetcher, error) {
			return logFetcher, nil
		},
		logging.Testing(),
		GlobalExporterRunnerConfig{
			PullInterval:       50 * time.Millisecond,
			PushRetryPeriod:    50 * time.Millisecond,
			LogsPageSize:       100,
			LedgerPullInterval: 50 * time.Millisecond,
		},
	)

	go runner.Run(ctx)
	close(deliver)

	require.Eventually(t, func() bool {
		select {
		case call := <-updateCh:
			return call.ledger == "ledger1" && call.lastLogID == 2
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond)

	driver.EXPECT().Stop(gomock.Any()).Return(nil)
	require.NoError(t, runner.Shutdown(ctx))

	require.Eventually(t, ctrl.Satisfied, 2*time.Second, 10*time.Millisecond)
}

func TestGlobalExporterRunnerReset(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	driver := drivers.NewMockDriver(ctrl)
	logFetcher := NewMockLogFetcher(ctrl)

	logFetcher.EXPECT().
		ListLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, _ common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return &bunpaginate.Cursor[ledger.Log]{}, nil
			}
		})

	driver.EXPECT().Start(gomock.Any()).Return(nil)

	deleteCalled := make(chan struct{})

	store := &testGlobalExporterStateStore{
		ledgersResource: &mockPaginatedResource{
			paginateFn: func(ctx context.Context, query common.PaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error) {
				return &bunpaginate.Cursor[ledger.Ledger]{
					Data: []ledger.Ledger{{ID: 1, Name: "ledger1"}},
				}, nil
			},
		},
		states:       map[string]uint64{"ledger1": 5},
		deleteCalled: deleteCalled,
	}

	runner := NewGlobalExporterRunner(
		store,
		driver,
		func(ctx context.Context, name string) (LogFetcher, error) {
			return logFetcher, nil
		},
		logging.Testing(),
		GlobalExporterRunnerConfig{
			PullInterval:       50 * time.Millisecond,
			PushRetryPeriod:    50 * time.Millisecond,
			LogsPageSize:       100,
			LedgerPullInterval: 50 * time.Millisecond,
			Reset:              true,
		},
	)

	go runner.Run(ctx)

	// Verify DeleteAllGlobalExporterStates was called
	select {
	case <-deleteCalled:
		// OK
	case <-time.After(5 * time.Second):
		require.Fail(t, "DeleteAllGlobalExporterStates should have been called")
	}

	driver.EXPECT().Stop(gomock.Any()).Return(nil)
	require.NoError(t, runner.Shutdown(ctx))
}

func TestGlobalExporterRunnerShutdownBeforeDriverReady(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	driver := drivers.NewMockDriver(gomock.NewController(t))

	// Driver.Start blocks forever (never becomes ready)
	driver.EXPECT().Start(gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	driver.EXPECT().Stop(gomock.Any()).AnyTimes().Return(nil)

	store := &testGlobalExporterStateStore{
		ledgersResource: &mockPaginatedResource{
			paginateFn: func(ctx context.Context, query common.PaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error) {
				return &bunpaginate.Cursor[ledger.Ledger]{}, nil
			},
		},
		states: map[string]uint64{},
	}

	runner := NewGlobalExporterRunner(
		store,
		driver,
		func(ctx context.Context, name string) (LogFetcher, error) {
			return nil, nil
		},
		logging.Testing(),
		GlobalExporterRunnerConfig{
			PullInterval:    50 * time.Millisecond,
			PushRetryPeriod: 50 * time.Millisecond,
		},
	)

	go runner.Run(ctx)

	// Give Run time to start, then shut down
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, runner.Shutdown(ctx))
}

func TestGlobalExporterRunnerShutdownTimeout(t *testing.T) {
	t.Parallel()

	store := &testGlobalExporterStateStore{
		states: map[string]uint64{},
	}

	runner := NewGlobalExporterRunner(
		store,
		nil,
		nil,
		logging.Testing(),
		GlobalExporterRunnerConfig{
			PullInterval: 50 * time.Millisecond,
		},
	)

	// Don't call Run — doneCh will never close
	// Shutdown with an already-expired context should return context error
	expiredCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := runner.Shutdown(expiredCtx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestGlobalExporterRunnerMultipleLedgers(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher1 := NewMockLogFetcher(ctrl)
	logFetcher2 := NewMockLogFetcher(ctrl)
	driver := drivers.NewMockDriver(ctrl)

	log1 := ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()})
	log1.ID = pointer.For(uint64(1))
	log2 := ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()})
	log2.ID = pointer.For(uint64(1))

	deliver := make(chan struct{})
	delivered1 := make(chan struct{})
	delivered2 := make(chan struct{})

	logFetcher1.EXPECT().
		ListLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, _ common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-deliver:
				select {
				case <-delivered1:
				default:
					close(delivered1)
					return &bunpaginate.Cursor[ledger.Log]{Data: []ledger.Log{log1}}, nil
				}
			}
			return &bunpaginate.Cursor[ledger.Log]{}, nil
		})

	logFetcher2.EXPECT().
		ListLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, _ common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-deliver:
				select {
				case <-delivered2:
				default:
					close(delivered2)
					return &bunpaginate.Cursor[ledger.Log]{Data: []ledger.Log{log2}}, nil
				}
			}
			return &bunpaginate.Cursor[ledger.Log]{}, nil
		})

	driver.EXPECT().Start(gomock.Any()).Return(nil)
	driver.EXPECT().
		Accept(gomock.Any(), drivers.NewLogWithLedger("ledger-a", log1)).
		Return([]error{nil}, nil)
	driver.EXPECT().
		Accept(gomock.Any(), drivers.NewLogWithLedger("ledger-b", log2)).
		Return([]error{nil}, nil)

	updateCh := make(chan updateStateCall, 2)

	store := &testGlobalExporterStateStore{
		ledgersResource: &mockPaginatedResource{
			paginateFn: func(ctx context.Context, query common.PaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error) {
				return &bunpaginate.Cursor[ledger.Ledger]{
					Data: []ledger.Ledger{
						{ID: 1, Name: "ledger-a"},
						{ID: 2, Name: "ledger-b"},
					},
				}, nil
			},
		},
		states:   map[string]uint64{},
		updateCh: updateCh,
	}

	runner := NewGlobalExporterRunner(
		store,
		driver,
		func(ctx context.Context, name string) (LogFetcher, error) {
			switch name {
			case "ledger-a":
				return logFetcher1, nil
			case "ledger-b":
				return logFetcher2, nil
			}
			return nil, errors.New("unknown ledger")
		},
		logging.Testing(),
		GlobalExporterRunnerConfig{
			PullInterval:       50 * time.Millisecond,
			PushRetryPeriod:    50 * time.Millisecond,
			LogsPageSize:       100,
			LedgerPullInterval: 50 * time.Millisecond,
		},
	)

	go runner.Run(ctx)
	close(deliver)

	// Collect two state updates
	updates := map[string]uint64{}
	require.Eventually(t, func() bool {
		select {
		case call := <-updateCh:
			updates[call.ledger] = call.lastLogID
		default:
		}
		return len(updates) >= 2
	}, 5*time.Second, 50*time.Millisecond)

	require.Equal(t, uint64(1), updates["ledger-a"])
	require.Equal(t, uint64(1), updates["ledger-b"])

	driver.EXPECT().Stop(gomock.Any()).Return(nil)
	require.NoError(t, runner.Shutdown(ctx))
}

func TestGlobalExporterRunnerListStatesError(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	driver := drivers.NewMockDriver(ctrl)

	driver.EXPECT().Start(gomock.Any()).Return(nil)
	driver.EXPECT().Stop(gomock.Any()).Return(nil)

	store := &testGlobalExporterStateStore{
		ledgersResource: &mockPaginatedResource{
			paginateFn: func(ctx context.Context, query common.PaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error) {
				return &bunpaginate.Cursor[ledger.Ledger]{}, nil
			},
		},
		listErr: errors.New("db error"),
	}

	runner := NewGlobalExporterRunner(
		store,
		driver,
		nil,
		logging.Testing(),
		GlobalExporterRunnerConfig{
			PullInterval:    50 * time.Millisecond,
			PushRetryPeriod: 50 * time.Millisecond,
		},
	)

	go runner.Run(ctx)

	// Run should exit early due to ListGlobalExporterStates error
	// doneCh should close
	select {
	case <-runner.doneCh:
		// OK — Run exited
	case <-time.After(5 * time.Second):
		require.Fail(t, "Run should have exited after ListGlobalExporterStates error")
	}
}

func TestGlobalExporterRunnerFetcherFailureDoesNotSkipLedger(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	driver := drivers.NewMockDriver(ctrl)

	fooLog := ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()})
	fooLog.ID = pointer.For(uint64(1))
	barLog := ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()})
	barLog.ID = pointer.For(uint64(1))
	bazLog := ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()})
	bazLog.ID = pointer.For(uint64(1))

	deliver := make(chan struct{})

	newTestFetcher := func(log ledger.Log) *MockLogFetcher {
		delivered := make(chan struct{})
		fetcher := NewMockLogFetcher(ctrl)
		fetcher.EXPECT().
			ListLogs(gomock.Any(), gomock.Any()).
			AnyTimes().
			DoAndReturn(func(ctx context.Context, _ common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-deliver:
					select {
					case <-delivered:
					default:
						close(delivered)
						return &bunpaginate.Cursor[ledger.Log]{Data: []ledger.Log{log}}, nil
					}
				}
				return &bunpaginate.Cursor[ledger.Log]{}, nil
			})
		return fetcher
	}

	fooFetcher := newTestFetcher(fooLog)
	barFetcher := newTestFetcher(barLog)
	bazFetcher := newTestFetcher(bazLog)

	driver.EXPECT().Start(gomock.Any()).Return(nil)
	driver.EXPECT().
		Accept(gomock.Any(), drivers.NewLogWithLedger("foo", fooLog)).
		Return([]error{nil}, nil)
	driver.EXPECT().
		Accept(gomock.Any(), drivers.NewLogWithLedger("bar", barLog)).
		Return([]error{nil}, nil)
	driver.EXPECT().
		Accept(gomock.Any(), drivers.NewLogWithLedger("baz", bazLog)).
		Return([]error{nil}, nil)

	updateCh := make(chan updateStateCall, 3)

	// Simulate: ledgers [id=1 "foo"], [id=2 "bar"], [id=3 "baz"]
	// First call to getLogFetcher("foo") fails, then succeeds on retry.
	fetcherCallCount := 0
	openLedger := func(ctx context.Context, name string) (LogFetcher, error) {
		if name == "foo" {
			fetcherCallCount++
			if fetcherCallCount == 1 {
				return nil, errors.New("temporary error")
			}
			return fooFetcher, nil
		}
		if name == "bar" {
			return barFetcher, nil
		}
		return bazFetcher, nil
	}

	store := &testGlobalExporterStateStore{
		ledgersResource: &mockPaginatedResource{
			paginateFn: func(ctx context.Context, query common.PaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error) {
				return &bunpaginate.Cursor[ledger.Ledger]{
					Data: []ledger.Ledger{
						{ID: 1, Name: "foo"},
						{ID: 2, Name: "bar"},
						{ID: 3, Name: "baz"},
					},
				}, nil
			},
		},
		states:   map[string]uint64{},
		updateCh: updateCh,
	}

	runner := NewGlobalExporterRunner(
		store,
		driver,
		openLedger,
		logging.Testing(),
		GlobalExporterRunnerConfig{
			PullInterval:       50 * time.Millisecond,
			PushRetryPeriod:    50 * time.Millisecond,
			LogsPageSize:       100,
			LedgerPullInterval: 50 * time.Millisecond,
		},
	)

	go runner.Run(ctx)
	close(deliver)

	// All three ledgers should eventually export their logs
	updates := map[string]uint64{}
	require.Eventually(t, func() bool {
		select {
		case call := <-updateCh:
			updates[call.ledger] = call.lastLogID
		default:
		}
		return len(updates) >= 3
	}, 5*time.Second, 50*time.Millisecond)

	require.Equal(t, uint64(1), updates["foo"])
	require.Equal(t, uint64(1), updates["bar"])
	require.Equal(t, uint64(1), updates["baz"])

	// foo's fetcher was called at least twice (first failure, then success)
	require.GreaterOrEqual(t, fetcherCallCount, 2)

	driver.EXPECT().Stop(gomock.Any()).Return(nil)
	require.NoError(t, runner.Shutdown(ctx))
}

func TestGlobalExporterRunnerShutdownIdempotent(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	driver := drivers.NewMockDriver(gomock.NewController(t))

	driver.EXPECT().Start(gomock.Any()).Return(nil)
	driver.EXPECT().Stop(gomock.Any()).Return(nil)

	store := &testGlobalExporterStateStore{
		ledgersResource: &mockPaginatedResource{
			paginateFn: func(ctx context.Context, query common.PaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error) {
				return &bunpaginate.Cursor[ledger.Ledger]{}, nil
			},
		},
		states: map[string]uint64{},
	}

	runner := NewGlobalExporterRunner(
		store,
		driver,
		nil,
		logging.Testing(),
		GlobalExporterRunnerConfig{
			PullInterval: 50 * time.Millisecond,
		},
	)

	go runner.Run(ctx)

	// Give it time to start
	time.Sleep(100 * time.Millisecond)

	// Multiple shutdown calls should not panic
	require.NoError(t, runner.Shutdown(ctx))
	require.NoError(t, runner.Shutdown(ctx))
}
