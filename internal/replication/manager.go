package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/otlp"
	"github.com/formancehq/go-libs/v4/platform/postgres"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/replication/drivers"
)

type Manager struct {
	mu sync.Mutex

	stopChannel        chan chan error
	storage            Storage
	pipelines          map[string]*PipelineHandler
	pipelinesWaitGroup sync.WaitGroup
	logger             logging.Logger

	driverFactory drivers.Factory
	drivers       map[string]*DriverFacade

	pipelineOptions          []PipelineOption
	exportersConfigValidator ConfigValidator
	syncPeriod               time.Duration
	started                  chan struct{}
}

func (m *Manager) CreateExporter(ctx context.Context, configuration ledger.ExporterConfiguration) (*ledger.Exporter, error) {
	if err := m.exportersConfigValidator.ValidateConfig(configuration.Driver, configuration.Config); err != nil {
		return nil, system.NewErrInvalidDriverConfiguration(configuration.Driver, err)
	}

	exporter := ledger.NewExporter(configuration)
	if err := m.storage.CreateExporter(ctx, exporter); err != nil {
		return nil, err
	}
	return &exporter, nil
}

func (m *Manager) initExporter(exporterID string) error {

	_, ok := m.drivers[exporterID]
	if ok {
		return nil
	}

	driver, _, err := m.driverFactory.Create(context.Background(), exporterID)
	if err != nil {
		return err
	}

	driverFacade := newDriverFacade(driver, m.logger, 2*time.Second)
	driverFacade.Run(context.Background())

	m.drivers[exporterID] = driverFacade

	return nil
}

func (m *Manager) stopDriver(ctx context.Context, driver drivers.Driver) {
	if err := driver.Stop(ctx); err != nil {
		m.logger.Errorf("stopping driver: %s", err)
	}
	for name, registeredExporter := range m.drivers {
		if driver == registeredExporter {
			delete(m.drivers, name)
			return
		}
	}
}

func (m *Manager) StartPipeline(ctx context.Context, pipelineID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	pipeline, err := m.storage.GetPipeline(ctx, pipelineID)
	if err != nil {
		return err
	}

	_, err = m.startPipeline(ctx, *pipeline)
	return err
}

func (m *Manager) startPipeline(ctx context.Context, pipeline ledger.Pipeline) (*PipelineHandler, error) {
	m.logger.Infof("initializing pipeline")
	_, ok := m.pipelines[pipeline.ID]
	if ok {
		return nil, ledger.NewErrAlreadyStarted(pipeline.ID)
	}

	ctx = logging.ContextWithLogger(
		ctx,
		m.logger.WithFields(map[string]any{
			"ledger":   pipeline.Ledger,
			"exporter": pipeline.ExporterID,
		}),
	)

	// Detach the context as once the process of pipeline initialisation is started, we must not stop it
	ctx = context.WithoutCancel(ctx)

	m.logger.Infof("initializing exporter")
	if err := m.initExporter(pipeline.ExporterID); err != nil {
		return nil, fmt.Errorf("initializing exporter: %w", err)
	}

	store, _, err := m.storage.OpenLedger(ctx, pipeline.Ledger)
	if err != nil {
		return nil, errors.Wrap(err, "opening ledger")
	}

	pipelineHandler := NewPipelineHandler(
		pipeline,
		store,
		m.drivers[pipeline.ExporterID],
		m.logger,
		m.pipelineOptions...,
	)
	m.pipelines[pipeline.ID] = pipelineHandler
	m.pipelinesWaitGroup.Add(1)

	// ignore the cancel function, as it will be called by the pipeline at its end
	subscription := make(chan uint64)

	m.logger.Infof("starting handler")
	go func() {
		for lastLogID := range subscription {
			if err := m.storage.StorePipelineState(ctx, pipeline.ID, lastLogID); err != nil {
				m.logger.Errorf("Unable to store state: %s", err)
			}
		}
	}()
	go func() {
		defer func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			defer m.pipelinesWaitGroup.Done()
			close(subscription)
		}()
		pipelineHandler.Run(ctx, subscription)
	}()

	return pipelineHandler, nil
}

func (m *Manager) stopPipeline(ctx context.Context, id string) error {
	handler, ok := m.pipelines[id]
	if !ok {
		return ledger.NewErrPipelineNotFound(id)
	}

	if err := handler.Shutdown(ctx); err != nil {
		return fmt.Errorf("error stopping pipeline: %w", err)
	}
	delete(m.pipelines, id)

	m.logger.Infof("pipeline terminated, pruning exporter...")
	m.stopExporterIfNeeded(ctx, handler)

	return nil
}

func (m *Manager) StopPipeline(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.stopPipeline(ctx, id)
}

func (m *Manager) stopPipelines(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id := range m.pipelines {
		if err := m.stopPipeline(ctx, id); err != nil {
			m.logger.Errorf("error stopping pipeline: %s", err)
		}
	}
}

func (m *Manager) stopExporterIfNeeded(ctx context.Context, handler *PipelineHandler) {
	// Check if the exporter associated to the pipeline is still in used
	exporter := handler.exporter
	for _, anotherPipeline := range m.pipelines {
		if anotherPipeline.exporter == exporter {
			// Exporter still used, keep it
			return
		}
	}

	m.logger.Infof("exporter %s no more used, stopping it...", handler.pipeline.ExporterID)
	m.stopDriver(ctx, exporter)
}

func (m *Manager) synchronizePipelines(ctx context.Context) error {
	m.logger.Debug("restore pipelines from store")
	defer func() {
		m.logger.Debug("restoration terminated")
	}()
	pipelines, err := m.storage.ListEnabledPipelines(ctx)
	if err != nil {
		return fmt.Errorf("reading pipelines from store: %w", err)
	}

	for _, pipeline := range pipelines {
		m.logger.Debugf("restoring pipeline %s", pipeline.ID)
		if _, err := m.startPipeline(ctx, pipeline); err != nil {
			switch {
			case errors.Is(err, ledger.ErrAlreadyStarted("")):
				m.logger.Debugf("Pipeline already started, skipping")
			default:
				return err
			}
		}
	}

	// Stop pipelines that are now disabled (or deleted)
outerLoop:
	for id := range m.pipelines {
		for _, pipeline := range pipelines {
			if id == pipeline.ID {
				continue outerLoop
			}
		}

		if err := m.stopPipeline(ctx, id); err != nil {
			m.logger.Errorf("error stopping pipeline: %s", err)
			continue
		}
	}

	return nil
}

func (m *Manager) Started() <-chan struct{} {
	return m.started
}

func (m *Manager) Run(ctx context.Context) {

	withLock := func(f func()) {
		m.mu.Lock()
		defer m.mu.Unlock()
		f()
	}

	withLock(func() {
		if err := m.synchronizePipelines(ctx); err != nil {
			m.logger.Errorf("restoring pipeline: %s", err)
		}
	})

	close(m.started)

	// copy to prevent data race when setting the stopChannel to nil
	stopChannel := m.stopChannel
	for {
		select {
		case signalChannel := <-stopChannel:
			m.logger.Debugf("got stop signal")
			m.stopPipelines(ctx)
			m.pipelinesWaitGroup.Wait()
			close(signalChannel)
			return
		case <-time.After(m.syncPeriod):
			withLock(func() {
				if err := m.synchronizePipelines(ctx); err != nil {
					m.logger.Errorf("synchronizing pipelines: %s", err)
				}
			})
		}
	}
}

func (m *Manager) GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error) {
	pipeline, err := m.storage.GetPipeline(ctx, id)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil, ledger.NewErrPipelineNotFound(id)
		}
		return nil, err
	}

	return pipeline, nil
}

func (m *Manager) Stop(ctx context.Context) error {
	m.logger.Info("stopping manager")
	signalChannel := make(chan error, 1)

	select {
	case m.stopChannel <- signalChannel:
		close(m.stopChannel)
		m.logger.Debug("stopping manager signal sent")
		select {
		case <-signalChannel:
			m.logger.Info("manager stopped")
			return nil
		case <-ctx.Done():
			m.logger.Error("context canceled while waiting for manager termination")
			return ctx.Err()
		}
	case <-ctx.Done():
		m.logger.Error("context canceled while waiting for manager signal handling")
		return ctx.Err()
	}
}

func (m *Manager) GetDriver(name string) *DriverFacade {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.drivers[name]
}

func (m *Manager) GetExporter(ctx context.Context, id string) (*ledger.Exporter, error) {
	exporter, err := m.storage.GetExporter(ctx, id)
	if err != nil {
		switch {
		case errors.Is(err, postgres.ErrNotFound):
			return nil, system.NewErrExporterNotFound(id)
		default:
			return nil, err
		}
	}
	return exporter, nil
}

func (m *Manager) ListExporters(ctx context.Context) (*bunpaginate.Cursor[ledger.Exporter], error) {
	return m.storage.ListExporters(ctx)
}

func (m *Manager) stopExporter(ctx context.Context, exporterID string) error {

	for id, config := range m.pipelines {
		if config.pipeline.ExporterID == exporterID {
			if err := m.stopPipeline(ctx, id); err != nil {
				return fmt.Errorf("stopping pipeline: %w", err)
			}
		}
	}

	return nil
}

func (m *Manager) DeleteExporter(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.stopExporter(ctx, id); err != nil {
		return err
	}

	if err := m.storage.DeleteExporter(ctx, id); err != nil {
		switch {
		case errors.Is(err, postgres.ErrNotFound):
			return system.NewErrExporterNotFound(id)
		default:
			return err
		}
	}
	return nil
}

func (m *Manager) UpdateExporter(ctx context.Context, id string, configuration ledger.ExporterConfiguration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.exportersConfigValidator.ValidateConfig(configuration.Driver, configuration.Config); err != nil {
		return system.NewErrInvalidDriverConfiguration(configuration.Driver, err)
	}

	if err := m.stopExporter(ctx, id); err != nil {
		return err
	}

	exporter, err := m.storage.GetExporter(ctx, id)
	if err != nil {
		switch {
		case errors.Is(err, postgres.ErrNotFound):
			return system.NewErrExporterNotFound(id)
		default:
			return err
		}
	}
	exporter.ExporterConfiguration = configuration

	if err := m.storage.UpdateExporter(ctx, *exporter); err != nil {
		return err
	}

	return m.synchronizePipelines(ctx)
}

func (m *Manager) ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error) {
	return m.storage.ListPipelines(ctx)
}

func (m *Manager) CreatePipeline(ctx context.Context, config ledger.PipelineConfiguration) (*ledger.Pipeline, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pipeline := ledger.NewPipeline(config)

	err := m.storage.CreatePipeline(ctx, pipeline)
	if err != nil {
		return nil, err
	}

	if _, err := m.startPipeline(ctx, pipeline); err != nil {
		logging.FromContext(ctx).Error("starting pipeline %s: %s", pipeline.ID, err)
		otlp.RecordError(ctx, err)
	}

	return &pipeline, nil
}

func (m *Manager) DeletePipeline(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.stopPipeline(ctx, id); err != nil {
		return err
	}

	if err := m.storage.DeletePipeline(ctx, id); err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return ledger.NewErrPipelineNotFound(id)
		}
		return err
	}

	return nil
}

func (m *Manager) ResetPipeline(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	started := m.pipelines[id] != nil

	if started {
		if err := m.stopPipeline(ctx, id); err != nil {
			return fmt.Errorf("stopping pipeline: %w", err)
		}
	}

	pipeline, err := m.storage.UpdatePipeline(ctx, id, map[string]any{
		"enabled":     true,
		"last_log_id": nil,
	})
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return ledger.NewErrPipelineNotFound(id)
		}
		return fmt.Errorf("updating pipeline: %w", err)
	}

	if started {
		if _, err := m.startPipeline(ctx, *pipeline); err != nil {
			logging.FromContext(ctx).Error("starting pipeline %s: %s", pipeline.ID, err)
		}
	}
	return nil
}

func NewManager(
	storageDriver Storage,
	driverFactory drivers.Factory,
	logger logging.Logger,
	exportersConfigValidator ConfigValidator,
	options ...Option,
) *Manager {
	ret := &Manager{
		storage:                  storageDriver,
		stopChannel:              make(chan chan error, 1),
		pipelines:                map[string]*PipelineHandler{},
		driverFactory:            driverFactory,
		drivers:                  map[string]*DriverFacade{},
		logger:                   logger.WithField("component", "manager"),
		exportersConfigValidator: exportersConfigValidator,
		started:                  make(chan struct{}),
	}

	for _, option := range append(defaultOptions, options...) {
		option(ret)
	}

	return ret
}

type Option func(r *Manager)

func WithPipelineOptions(options ...PipelineOption) Option {
	return func(r *Manager) {
		r.pipelineOptions = append(r.pipelineOptions, options...)
	}
}

func WithSyncPeriod(period time.Duration) Option {
	return func(r *Manager) {
		r.syncPeriod = period
	}
}

var defaultOptions = []Option{
	WithSyncPeriod(time.Minute),
}
