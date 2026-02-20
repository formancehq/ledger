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
			Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
		}).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, paginatedQuery common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
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
			Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
		}).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, paginatedQuery common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
			return &bunpaginate.Cursor[ledger.Log]{}, nil
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
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
			}).
			AnyTimes().
			DoAndReturn(func(ctx context.Context, paginatedQuery common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
				return &bunpaginate.Cursor[ledger.Log]{}, nil
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
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
			}).
			AnyTimes().
			DoAndReturn(func(ctx context.Context, paginatedQuery common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
				return &bunpaginate.Cursor[ledger.Log]{}, nil
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
