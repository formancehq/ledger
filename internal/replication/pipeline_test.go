package replication

import (
	"context"
	"errors"
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

	fetchCount := 0
	logFetcher.EXPECT().
		ListLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, _ common.PaginatedQuery[any]) (*paginate.Cursor[ledger.Log], error) {
			fetchCount++
			if fetchCount == 1 {
				return nil, errors.New("store unavailable")
			}
			if fetchCount == 2 {
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
