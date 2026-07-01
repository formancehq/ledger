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

func startManager(
	t *testing.T,
	ctx context.Context,
	storageDriver Storage,
	driverFactory drivers.Factory,
	exportersConfigValidator ConfigValidator,
) *Manager {
	t.Helper()

	manager := NewManager(
		storageDriver,
		driverFactory,
		logging.Testing(),
		exportersConfigValidator,
		WithSyncPeriod(time.Second),
	)
	go manager.Run(ctx)

	return manager
}

func TestWithSyncPeriodMinimum(t *testing.T) {
	t.Parallel()

	m := &Manager{}
	WithSyncPeriod(1)(m)
	require.Equal(t, time.Duration(2), m.syncPeriod)
}

func TestNewDriverFacadeMinimumRetryInterval(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	driver := drivers.NewMockDriver(ctrl)

	facade := newDriverFacade(driver, logging.Testing(), 1)
	require.Equal(t, time.Duration(2), facade.retryInterval)
}

// TestDriverFacadeStopContextExpiresDuringStart covers the ctx.Done branch in
// DriverFacade.Stop: when Stop is called while the start goroutine is still
// running and the stop context expires before the goroutine finishes, Stop must
// return ctx.Err().
func TestDriverFacadeStopContextExpiresDuringStart(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	blockStart := make(chan struct{})
	startedChan := make(chan struct{}, 1)

	mockDriver := drivers.NewMockDriver(ctrl)
	// Start blocks on blockStart regardless of context cancellation, keeping
	// startingChan open while Stop's inner select runs.
	mockDriver.EXPECT().Start(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
		startedChan <- struct{}{}
		<-blockStart
		return ctx.Err()
	}).AnyTimes()

	facade := newDriverFacade(mockDriver, logging.Testing(), time.Minute)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	facade.Run(runCtx)

	// Wait for Start to actually begin before proceeding.
	<-startedChan

	// Pre-cancel the stop context so the inner select picks ctx.Done()
	// before startingChan can close.
	stopCtx, cancelStop := context.WithCancel(context.Background())
	cancelStop()

	err := facade.Stop(stopCtx)
	require.ErrorIs(t, err, context.Canceled)

	// Unblock the start goroutine so it can exit cleanly.
	close(blockStart)
}

// TestDriverFacadeRetryThenSuccess covers the retry-timer branch in
// DriverFacade.Run: Start fails with a non-context error, the jittered timer
// fires, and Start succeeds on the second attempt.
func TestDriverFacadeRetryThenSuccess(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockDriver := drivers.NewMockDriver(ctrl)

	gomock.InOrder(
		mockDriver.EXPECT().Start(gomock.Any()).Return(errors.New("transient")),
		mockDriver.EXPECT().Start(gomock.Any()).Return(nil),
	)

	facade := newDriverFacade(mockDriver, logging.Testing(), 2)
	facade.Run(context.Background())

	select {
	case <-facade.Ready():
	case <-time.After(time.Second):
		require.Fail(t, "facade should become ready after retry")
	}

	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}

// TestDriverFacadeAcceptNotReady covers the "not ready exporter" branch in
// DriverFacade.Accept (driver_facade.go line 86): calling Accept before the
// driver has started returns an error.
func TestDriverFacadeAcceptNotReady(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockDriver := drivers.NewMockDriver(ctrl)

	// Start blocks forever so readyChan is never closed.
	mockDriver.EXPECT().Start(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}).AnyTimes()

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	facade := newDriverFacade(mockDriver, logging.Testing(), time.Minute)
	facade.Run(runCtx)

	_, err := facade.Accept(context.Background())
	require.Error(t, err)
	require.Equal(t, "not ready exporter", err.Error())
}

func TestManagerExportersNominal(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)
	logFetcher := NewMockLogFetcher(ctrl)
	exporterConfigValidator := NewMockConfigValidator(ctrl)
	driverFactory := drivers.NewMockFactory(ctrl)
	driver := drivers.NewMockDriver(ctrl)

	pipelineConfiguration := ledger.NewPipelineConfiguration("module1", "exporter")
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	driverFactory.EXPECT().
		Create(gomock.Any(), pipelineConfiguration.ExporterID).
		Return(driver, nil, nil)
	driver.EXPECT().Start(gomock.Any()).Return(nil)

	log := ledger.NewLog(ledger.CreatedTransaction{
		Transaction: ledger.NewTransaction(),
	})
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

	storage.EXPECT().
		ListEnabledPipelines(gomock.Any()).
		AnyTimes().
		Return([]ledger.Pipeline{pipeline}, nil)

	storage.EXPECT().
		GetPipeline(gomock.Any(), pipeline.ID).
		Return(&pipeline, nil)

	storage.EXPECT().
		OpenLedger(gomock.Any(), pipelineConfiguration.Ledger).
		Return(logFetcher, &ledger.Ledger{}, nil)

	storage.EXPECT().
		StorePipelineState(gomock.Any(), pipeline.ID, uint64(1)).
		Return(nil)

	driver.EXPECT().
		Accept(gomock.Any(), drivers.NewLogWithLedger(pipelineConfiguration.Ledger, log)).
		Return([]error{nil}, nil)

	manager := startManager(
		t,
		ctx,
		storage,
		driverFactory,
		exporterConfigValidator,
	)
	t.Cleanup(func() {
		require.NoError(t, manager.Stop(ctx))
	})
	<-manager.Started()

	err := manager.StartPipeline(ctx, pipeline.ID)
	require.Error(t, err)

	require.Eventually(t, func() bool {
		return manager.GetDriver("exporter") != nil
	}, 5*time.Second, 10*time.Millisecond)

	select {
	case <-manager.GetDriver("exporter").Ready():
	case <-time.After(time.Second):
		require.Fail(t, "exporter should be ready")
	}

	close(deliver)

	require.Eventually(t, ctrl.Satisfied, 2*time.Second, 10*time.Millisecond)

	// notes(gfyrag): add this expectation AFTER the previous Eventually.
	// If configured before the Eventually, it will never finish as the stop call is made in a t.Cleanup defined earlier
	driver.EXPECT().Stop(gomock.Any()).Return(nil)
}

func TestManagerExportersUpdate(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)
	logFetcher := NewMockLogFetcher(ctrl)
	exporterConfigValidator := NewMockConfigValidator(ctrl)
	driverFactory := drivers.NewMockFactory(ctrl)
	driver := drivers.NewMockDriver(ctrl)

	pipelineConfiguration := ledger.NewPipelineConfiguration("module1", "exporter")
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	driverFactory.EXPECT().
		Create(gomock.Any(), pipelineConfiguration.ExporterID).
		AnyTimes().
		Return(driver, nil, nil)
	driver.EXPECT().
		Start(gomock.Any()).
		AnyTimes().
		Return(nil)

	log := ledger.NewLog(ledger.CreatedTransaction{
		Transaction: ledger.NewTransaction(),
	})
	log.ID = pointer.For(uint64(1))

	logFetcher.EXPECT().
		ListLogs(gomock.Any(), common.InitialPaginatedQuery[any]{
			PageSize: 100,
			Column:   "id",
			Options:  common.ResourceQuery[any]{},
			Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
		}).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, paginatedQuery common.PaginatedQuery[any]) (*paginate.Cursor[ledger.Log], error) {
			return &paginate.Cursor[ledger.Log]{}, nil
		})

	storage.EXPECT().
		ListEnabledPipelines(gomock.Any()).
		AnyTimes().
		Return([]ledger.Pipeline{pipeline}, nil)

	storage.EXPECT().
		GetPipeline(gomock.Any(), pipeline.ID).
		AnyTimes().
		Return(&pipeline, nil)

	storage.EXPECT().
		OpenLedger(gomock.Any(), pipelineConfiguration.Ledger).
		AnyTimes().
		Return(logFetcher, &ledger.Ledger{}, nil)

	manager := startManager(
		t,
		ctx,
		storage,
		driverFactory,
		exporterConfigValidator,
	)
	t.Cleanup(func() {
		require.NoError(t, manager.Stop(ctx))
	})
	<-manager.Started()

	err := manager.StartPipeline(ctx, pipeline.ID)
	require.Error(t, err)

	require.Eventually(t, func() bool {
		return manager.GetDriver("exporter") != nil
	}, 5*time.Second, 10*time.Millisecond)

	select {
	case <-manager.GetDriver("exporter").Ready():
	case <-time.After(time.Second):
		require.Fail(t, "exporter should be ready")
	}

	require.Eventually(t, ctrl.Satisfied, 2*time.Second, 10*time.Millisecond)

	exporterConfigValidator.EXPECT().
		ValidateConfig(gomock.Any(), gomock.Any()).
		Return(nil)

	driver.EXPECT().Stop(gomock.Any()).Return(nil)

	storage.EXPECT().
		GetExporter(gomock.Any(), pipelineConfiguration.ExporterID).
		Return(&ledger.Exporter{}, nil)

	storage.EXPECT().
		UpdateExporter(gomock.Any(), gomock.Any()).
		Return(nil)

	err = manager.UpdateExporter(ctx, "exporter", ledger.ExporterConfiguration{})
	require.NoError(t, err)

	require.Eventually(t, ctrl.Satisfied, 2*time.Second, 10*time.Millisecond)

	// The polling should automatically restart the exporter
	require.Eventually(t, func() bool {
		return manager.GetDriver("exporter") != nil
	}, 5*time.Second, 10*time.Millisecond)

	// notes(gfyrag): add this expectation AFTER the previous Eventually.
	// If configured before the Eventually, it will never finish as the stop call is made in a t.Cleanup defined earlier
	driver.EXPECT().Stop(gomock.Any()).Return(nil)
}

func TestManagerStop(t *testing.T) {
	t.Parallel()

	t.Run("nominal", func(t *testing.T) {
		ctx := logging.TestingContext()
		ctrl := gomock.NewController(t)
		storage := NewMockStorage(ctrl)
		logFetcher := NewMockLogFetcher(ctrl)
		exporterConfigValidator := NewMockConfigValidator(ctrl)
		driverFactory := drivers.NewMockFactory(ctrl)
		driver := drivers.NewMockDriver(ctrl)

		pipelineConfiguration := ledger.NewPipelineConfiguration("module1", "exporter")
		pipeline := ledger.NewPipeline(pipelineConfiguration)

		driverFactory.EXPECT().
			Create(gomock.Any(), pipelineConfiguration.ExporterID).
			AnyTimes().
			Return(driver, nil, nil)
		driver.EXPECT().
			Start(gomock.Any()).
			AnyTimes().
			Return(nil)

		log := ledger.NewLog(ledger.CreatedTransaction{
			Transaction: ledger.NewTransaction(),
		})
		log.ID = pointer.For(uint64(1))

		logFetcher.EXPECT().
			ListLogs(gomock.Any(), common.InitialPaginatedQuery[any]{
				PageSize: 100,
				Column:   "id",
				Options:  common.ResourceQuery[any]{},
				Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
			}).
			AnyTimes().
			DoAndReturn(func(ctx context.Context, paginatedQuery common.PaginatedQuery[any]) (*paginate.Cursor[ledger.Log], error) {
				return &paginate.Cursor[ledger.Log]{}, nil
			})

		storage.EXPECT().
			ListEnabledPipelines(gomock.Any()).
			AnyTimes().
			Return([]ledger.Pipeline{pipeline}, nil)

		storage.EXPECT().
			GetPipeline(gomock.Any(), pipeline.ID).
			AnyTimes().
			Return(&pipeline, nil)

		storage.EXPECT().
			OpenLedger(gomock.Any(), pipelineConfiguration.Ledger).
			AnyTimes().
			Return(logFetcher, &ledger.Ledger{}, nil)

		manager := startManager(
			t,
			ctx,
			storage,
			driverFactory,
			exporterConfigValidator,
		)
		<-manager.Started()

		err := manager.StartPipeline(ctx, pipeline.ID)
		require.Error(t, err)

		require.Eventually(t, func() bool {
			return manager.GetDriver("exporter") != nil
		}, 5*time.Second, 10*time.Millisecond)

		select {
		case <-manager.GetDriver("exporter").Ready():
		case <-time.After(time.Second):
			require.Fail(t, "exporter should be ready")
		}

		require.Eventually(t, ctrl.Satisfied, 2*time.Second, 10*time.Millisecond)

		// notes(gfyrag): add this expectation AFTER the previous Eventually.
		// If configured before the Eventually, it will never finish as the stop call is made in a t.Cleanup defined earlier
		driver.EXPECT().Stop(gomock.Any()).Return(nil)

		require.NoError(t, manager.Stop(ctx))
	})
	t.Run("error on driver initialization", func(t *testing.T) {
		ctx := logging.TestingContext()
		ctrl := gomock.NewController(t)
		storage := NewMockStorage(ctrl)
		logFetcher := NewMockLogFetcher(ctrl)
		exporterConfigValidator := NewMockConfigValidator(ctrl)
		driverFactory := drivers.NewMockFactory(ctrl)
		driver := drivers.NewMockDriver(ctrl)

		pipelineConfiguration := ledger.NewPipelineConfiguration("module1", "exporter")
		pipeline := ledger.NewPipeline(pipelineConfiguration)

		logFetcher.EXPECT().
			ListLogs(gomock.Any(), common.InitialPaginatedQuery[any]{
				PageSize: 100,
				Column:   "id",
				Options:  common.ResourceQuery[any]{},
				Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
			}).
			AnyTimes().
			DoAndReturn(func(ctx context.Context, paginatedQuery common.PaginatedQuery[any]) (*paginate.Cursor[ledger.Log], error) {
				return &paginate.Cursor[ledger.Log]{}, nil
			})

		driverFactory.EXPECT().
			Create(gomock.Any(), pipelineConfiguration.ExporterID).
			AnyTimes().
			Return(driver, nil, nil)
		driver.EXPECT().
			Start(gomock.Any()).
			AnyTimes().
			Return(errors.New("unknown error from driver initialization"))

		storage.EXPECT().
			ListEnabledPipelines(gomock.Any()).
			AnyTimes().
			Return([]ledger.Pipeline{pipeline}, nil)

		storage.EXPECT().
			GetPipeline(gomock.Any(), pipeline.ID).
			AnyTimes().
			Return(&pipeline, nil)

		storage.EXPECT().
			OpenLedger(gomock.Any(), pipelineConfiguration.Ledger).
			AnyTimes().
			Return(logFetcher, &ledger.Ledger{}, nil)

		manager := startManager(
			t,
			ctx,
			storage,
			driverFactory,
			exporterConfigValidator,
		)
		<-manager.Started()

		require.Eventually(t, func() bool {
			return manager.GetDriver("exporter") != nil
		}, 5*time.Second, 10*time.Millisecond)

		require.Eventually(t, ctrl.Satisfied, 2*time.Second, 10*time.Millisecond)

		require.NoError(t, manager.Stop(ctx))
	})

}

// TestManagerSynchronizePipelinesError covers manager.go lines 260-262:
// synchronizePipelines returns an error during Run's initial sync; the manager logs
// the error and continues running normally.
func TestManagerSynchronizePipelinesError(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)
	exporterConfigValidator := NewMockConfigValidator(ctrl)
	driverFactory := drivers.NewMockFactory(ctrl)

	storage.EXPECT().
		ListEnabledPipelines(gomock.Any()).
		AnyTimes().
		Return(nil, errors.New("database unavailable"))

	manager := startManager(t, ctx, storage, driverFactory, exporterConfigValidator)
	<-manager.Started()

	require.NoError(t, manager.Stop(ctx))
	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}

// TestManagerPeriodicSyncError covers manager.go line 280-282: the periodic
// sync timer fires, synchronizePipelines returns an error, and the manager
// logs the error and continues running.
func TestManagerPeriodicSyncError(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)
	exporterConfigValidator := NewMockConfigValidator(ctrl)
	driverFactory := drivers.NewMockFactory(ctrl)

	var callCount atomic.Int32
	storage.EXPECT().
		ListEnabledPipelines(gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context) ([]ledger.Pipeline, error) {
			n := callCount.Add(1)
			if n == 1 {
				return nil, nil
			}
			return nil, errors.New("periodic sync failure")
		})

	manager := NewManager(
		storage,
		driverFactory,
		logging.Testing(),
		exporterConfigValidator,
		WithSyncPeriod(2),
	)
	go manager.Run(ctx)
	<-manager.Started()

	require.Eventually(t, func() bool {
		return callCount.Load() >= 2
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, manager.Stop(ctx))
}

// TestManagerPeriodicSync covers the time.After branch in manager.go Run loop
// (line 278): the sync timer fires and triggers synchronizePipelines again.
func TestManagerPeriodicSync(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)
	exporterConfigValidator := NewMockConfigValidator(ctrl)
	driverFactory := drivers.NewMockFactory(ctrl)

	syncCount := make(chan struct{}, 10)
	storage.EXPECT().
		ListEnabledPipelines(gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context) ([]ledger.Pipeline, error) {
			select {
			case syncCount <- struct{}{}:
			default:
			}
			return nil, nil
		})

	manager := NewManager(
		storage,
		driverFactory,
		logging.Testing(),
		exporterConfigValidator,
		WithSyncPeriod(2),
	)
	go manager.Run(ctx)

	<-manager.Started()

	// First sync happened at startup. Wait for at least one periodic sync.
	<-syncCount // initial
	<-syncCount // periodic

	require.NoError(t, manager.Stop(ctx))
}
