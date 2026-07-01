package replication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/storage/common"
)

func runPipeline(t *testing.T, ctx context.Context, pipeline ledger.Pipeline, store LogFetcher, driver drivers.Driver) (*PipelineHandler, <-chan uint64) {
	t.Helper()

	handler := NewPipelineHandler(
		pipeline,
		store,
		driver,
		logging.Testing(),
	)

	lastLogIDChannel := make(chan uint64)

	go handler.Run(ctx, lastLogIDChannel)
	t.Cleanup(func() {
		require.NoError(t, handler.Shutdown(ctx))
	})

	return handler, lastLogIDChannel
}

func TestWithPullPeriodMinimum(t *testing.T) {
	t.Parallel()

	config := PipelineHandlerConfig{}
	WithPullPeriod(1)(&config)
	require.Equal(t, time.Duration(2), config.PullInterval)
}

func TestWithPushRetryPeriodMinimum(t *testing.T) {
	t.Parallel()

	config := PipelineHandlerConfig{}
	WithPushRetryPeriod(1)(&config)
	require.Equal(t, time.Duration(2), config.PushRetryPeriod)
}

func TestPipeline(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	driver := drivers.NewMockDriver(ctrl)
	log := ledger.NewLog(
		ledger.CreatedTransaction{
			Transaction: ledger.NewTransaction(),
		},
	)
	log.ID = pointer.For(uint64(1))

	deliver := make(chan struct{})
	delivered := make(chan struct{})

	logFetcher.EXPECT().
		ListLogs(gomock.Any(), common.InitialPaginatedQuery[any]{
			PageSize: 100,
			Column:   "id",
			Options:  common.ResourceQuery[any]{},
			Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
		}).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, paginatedQuery common.PaginatedQuery[any]) (*paginate.Cursor[ledger.Log], error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-deliver:
				select {
				case <-delivered:
				default:
					close(delivered)
					return &paginate.Cursor[ledger.Log]{
						Data: []ledger.Log{log},
					}, nil
				}
			}
			return &paginate.Cursor[ledger.Log]{}, nil
		})

	driver.EXPECT().
		Accept(gomock.Any(), drivers.NewLogWithLedger("testing", log)).
		Return([]error{nil}, nil)

	pipelineConfiguration := ledger.NewPipelineConfiguration("testing", "testing")
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	_, lastLogIDChannel := runPipeline(t, ctx, pipeline, logFetcher, driver)

	close(deliver)

	ShouldReceive(t, 1, lastLogIDChannel)

	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}

// TestPipelineStopDuringExport covers pipeline.go lines 144-147:
// stop signal received while the export Accept call is in progress.
func TestPipelineStopDuringExport(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	driver := drivers.NewMockDriver(ctrl)

	log := ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()})
	log.ID = pointer.For(uint64(1))

	deliver := make(chan struct{})
	logFetcher.EXPECT().
		ListLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, _ common.PaginatedQuery[any]) (*paginate.Cursor[ledger.Log], error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-deliver:
				return &paginate.Cursor[ledger.Log]{Data: []ledger.Log{log}}, nil
			}
		})

	// Accept blocks until its context is cancelled, keeping the export in-flight.
	acceptStarted := make(chan struct{})
	driver.EXPECT().
		Accept(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ ...drivers.LogWithLedger) ([]error, error) {
			close(acceptStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		})

	pipelineConfiguration := ledger.NewPipelineConfiguration("testing", "testing")
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	handler := NewPipelineHandler(pipeline, logFetcher, driver, logging.Testing())
	lastLogIDChannel := make(chan uint64, 1)
	go handler.Run(ctx, lastLogIDChannel)

	close(deliver)
	<-acceptStarted

	require.NoError(t, handler.Shutdown(ctx))
	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}

// TestPipelineHasMoreLogs covers pipeline.go lines 163-168:
// when the cursor reports HasMore, nextInterval is set to 0 for immediate re-fetch.
func TestPipelineHasMoreLogs(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	driver := drivers.NewMockDriver(ctrl)

	log1 := ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()})
	log1.ID = pointer.For(uint64(1))
	log2 := ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()})
	log2.ID = pointer.For(uint64(2))

	var fetchCount atomic.Int32
	logFetcher.EXPECT().
		ListLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, _ common.PaginatedQuery[any]) (*paginate.Cursor[ledger.Log], error) {
			n := fetchCount.Add(1)
			switch n {
			case 1:
				return &paginate.Cursor[ledger.Log]{
					Data:    []ledger.Log{log1},
					HasMore: true,
				}, nil
			case 2:
				return &paginate.Cursor[ledger.Log]{
					Data: []ledger.Log{log2},
				}, nil
			default:
				return &paginate.Cursor[ledger.Log]{}, nil
			}
		})

	driver.EXPECT().
		Accept(gomock.Any(), gomock.Any()).
		Times(2).
		Return([]error{nil}, nil)

	pipelineConfiguration := ledger.NewPipelineConfiguration("testing", "testing")
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	handler := NewPipelineHandler(pipeline, logFetcher, driver, logging.Testing())
	lastLogIDChannel := make(chan uint64, 2)
	go handler.Run(ctx, lastLogIDChannel)
	t.Cleanup(func() {
		require.NoError(t, handler.Shutdown(ctx))
	})

	ShouldReceive(t, uint64(1), lastLogIDChannel)
	ShouldReceive(t, uint64(2), lastLogIDChannel)
	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}

// TestPipelineShutdownContextExpired covers pipeline.go lines 177-178:
// Shutdown's context expires before the stop signal can be sent.
func TestPipelineShutdownContextExpired(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	driver := drivers.NewMockDriver(ctrl)

	logFetcher.EXPECT().
		ListLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, _ common.PaginatedQuery[any]) (*paginate.Cursor[ledger.Log], error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})

	pipelineConfiguration := ledger.NewPipelineConfiguration("testing", "testing")
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	handler := NewPipelineHandler(pipeline, logFetcher, driver, logging.Testing())

	// Fill the stop channel so Shutdown blocks waiting to send.
	handler.stopChannel <- make(chan error)

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	err := handler.Shutdown(cancelledCtx)
	require.ErrorIs(t, err, context.Canceled)

	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}

// TestPipelineShutdownWaitContextExpired covers pipeline.go lines 184-185:
// Shutdown sends the stop signal but the context expires before the pipeline
// confirms termination.
func TestPipelineShutdownWaitContextExpired(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	driver := drivers.NewMockDriver(ctrl)

	// Block ListLogs so the pipeline never processes the stop signal.
	logFetcher.EXPECT().
		ListLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, _ common.PaginatedQuery[any]) (*paginate.Cursor[ledger.Log], error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})

	pipelineConfiguration := ledger.NewPipelineConfiguration("testing", "testing")
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	handler := NewPipelineHandler(pipeline, logFetcher, driver, logging.Testing())

	lastLogIDChannel := make(chan uint64, 1)
	runCtx, cancelRun := context.WithCancel(ctx)
	go handler.Run(runCtx, lastLogIDChannel)

	// Give the pipeline a moment to enter its select loop.
	time.Sleep(10 * time.Millisecond)

	// Shutdown with an already-cancelled context: the stop signal can be sent
	// (stopChannel is buffered with size 1) but waiting for the error response
	// should hit the ctx.Done() branch.
	shutdownCtx, cancelShutdown := context.WithCancel(ctx)
	cancelShutdown()
	err := handler.Shutdown(shutdownCtx)
	require.ErrorIs(t, err, context.Canceled)

	cancelRun()
	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}

// TestPipelineFetchRetryThenSuccess covers pipeline.go lines 107-108:
// ListLogs errors once, the jittered retry timer fires, and the next ListLogs
// call succeeds.
func TestPipelineFetchRetryThenSuccess(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	driver := drivers.NewMockDriver(ctrl)

	log := ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()})
	log.ID = pointer.For(uint64(1))

	var fetchCount atomic.Int32
	logFetcher.EXPECT().
		ListLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, _ common.PaginatedQuery[any]) (*paginate.Cursor[ledger.Log], error) {
			n := fetchCount.Add(1)
			if n == 1 {
				return nil, errors.New("store unavailable")
			}
			if n == 2 {
				return &paginate.Cursor[ledger.Log]{Data: []ledger.Log{log}}, nil
			}
			return &paginate.Cursor[ledger.Log]{}, nil
		})

	driver.EXPECT().
		Accept(gomock.Any(), drivers.NewLogWithLedger("testing", log)).
		Return([]error{nil}, nil)

	pipelineConfiguration := ledger.NewPipelineConfiguration("testing", "testing")
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	handler := NewPipelineHandler(
		pipeline, logFetcher, driver, logging.Testing(),
		WithPullPeriod(2),
	)

	lastLogIDChannel := make(chan uint64, 1)
	go handler.Run(ctx, lastLogIDChannel)
	t.Cleanup(func() {
		require.NoError(t, handler.Shutdown(ctx))
	})

	ShouldReceive(t, uint64(1), lastLogIDChannel)
	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}

// TestPipelineStopDuringFetchRetry covers pipeline.go lines 104-106:
// stop signal received while waiting in the fetch-retry select after a ListLogs error.
func TestPipelineStopDuringFetchRetry(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	driver := drivers.NewMockDriver(ctrl)

	// fetchErrored is closed once the first ListLogs call returns an error.
	fetchErrored := make(chan struct{})
	logFetcher.EXPECT().
		ListLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, _ common.PaginatedQuery[any]) (*paginate.Cursor[ledger.Log], error) {
			select {
			case <-fetchErrored:
			default:
				close(fetchErrored)
			}
			return nil, errors.New("store unavailable")
		})

	pipelineConfiguration := ledger.NewPipelineConfiguration("testing", "testing")
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	// Long PullInterval keeps the pipeline in the retry-wait select until Shutdown fires.
	handler := NewPipelineHandler(
		pipeline, logFetcher, driver, logging.Testing(),
		WithPullPeriod(time.Hour),
	)

	lastLogIDChannel := make(chan uint64, 1)
	go handler.Run(ctx, lastLogIDChannel)

	<-fetchErrored
	require.NoError(t, handler.Shutdown(ctx))
	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}

// TestPipelineStopDuringPushRetry covers pipeline.go lines 137-139:
// stop signal received while waiting in the push-retry select after an Accept error.
func TestPipelineStopDuringPushRetry(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	driver := drivers.NewMockDriver(ctrl)

	log := ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()})
	log.ID = pointer.For(uint64(1))

	deliver := make(chan struct{})
	logFetcher.EXPECT().
		ListLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, _ common.PaginatedQuery[any]) (*paginate.Cursor[ledger.Log], error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-deliver:
				return &paginate.Cursor[ledger.Log]{Data: []ledger.Log{log}}, nil
			}
		})

	// acceptErrored is closed once the first Accept returns an error.
	acceptErrored := make(chan struct{})
	driver.EXPECT().
		Accept(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, _ ...drivers.LogWithLedger) ([]error, error) {
			select {
			case <-acceptErrored:
			default:
				close(acceptErrored)
			}
			return nil, errors.New("export failed")
		})

	pipelineConfiguration := ledger.NewPipelineConfiguration("testing", "testing")
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	// Long PushRetryPeriod keeps the pipeline in the push-retry select until Shutdown fires.
	handler := NewPipelineHandler(
		pipeline, logFetcher, driver, logging.Testing(),
		WithPushRetryPeriod(time.Hour),
	)

	lastLogIDChannel := make(chan uint64, 1)
	go handler.Run(ctx, lastLogIDChannel)

	close(deliver)
	<-acceptErrored
	require.NoError(t, handler.Shutdown(ctx))
	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}

// TestPipelinePushRetryThenSuccess covers pipeline.go lines 140-141:
// Accept errors once, the push-retry timer fires, Accept succeeds on the second attempt.
func TestPipelinePushRetryThenSuccess(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	driver := drivers.NewMockDriver(ctrl)

	log := ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()})
	log.ID = pointer.For(uint64(1))

	deliver := make(chan struct{})
	delivered := make(chan struct{})
	logFetcher.EXPECT().
		ListLogs(gomock.Any(), common.InitialPaginatedQuery[any]{
			PageSize: 100,
			Column:   "id",
			Options:  common.ResourceQuery[any]{},
			Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
		}).
		AnyTimes().
		DoAndReturn(func(_ context.Context, _ common.PaginatedQuery[any]) (*paginate.Cursor[ledger.Log], error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-deliver:
				select {
				case <-delivered:
				default:
					close(delivered)
					return &paginate.Cursor[ledger.Log]{Data: []ledger.Log{log}}, nil
				}
			}
			return &paginate.Cursor[ledger.Log]{}, nil
		})

	// First Accept call fails; second succeeds.
	gomock.InOrder(
		driver.EXPECT().
			Accept(gomock.Any(), drivers.NewLogWithLedger("testing", log)).
			Return(nil, errors.New("first attempt failed")),
		driver.EXPECT().
			Accept(gomock.Any(), drivers.NewLogWithLedger("testing", log)).
			Return([]error{nil}, nil),
	)

	pipelineConfiguration := ledger.NewPipelineConfiguration("testing", "testing")
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	// Minimum PushRetryPeriod (2ns) so the retry timer fires immediately.
	handler := NewPipelineHandler(
		pipeline, logFetcher, driver, logging.Testing(),
		WithPushRetryPeriod(2),
	)

	lastLogIDChannel := make(chan uint64, 1)
	go handler.Run(ctx, lastLogIDChannel)
	t.Cleanup(func() {
		require.NoError(t, handler.Shutdown(ctx))
	})

	close(deliver)

	ShouldReceive(t, uint64(1), lastLogIDChannel)
	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}
