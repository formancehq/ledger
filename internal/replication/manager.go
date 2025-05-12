package replication

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/controller/system"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/pkg/errors"
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

	pipelineOptions           []PipelineOption
	connectorsConfigValidator ConfigValidator
}

func (m *Manager) CreateConnector(ctx context.Context, configuration ledger.ConnectorConfiguration) (*ledger.Connector, error) {
	if err := m.connectorsConfigValidator.ValidateConfig(configuration.Driver, configuration.Config); err != nil {
		return nil, system.NewErrInvalidDriverConfiguration(configuration.Driver, err)
	}

	connector := ledger.NewConnector(configuration)
	if err := m.storage.CreateConnector(ctx, connector); err != nil {
		return nil, err
	}
	return &connector, nil
}

func (m *Manager) initConnector(connectorID string) error {

	_, ok := m.drivers[connectorID]
	if ok {
		return nil
	}

	driver, _, err := m.driverFactory.Create(context.Background(), connectorID)
	if err != nil {
		return err
	}

	driverFacade := newDriverFacade(driver, m.logger, 2*time.Second)
	driverFacade.Run(context.Background())

	m.drivers[connectorID] = driverFacade

	return nil
}

func (m *Manager) stopDriver(ctx context.Context, driver drivers.Driver) {
	if err := driver.Stop(ctx); err != nil {
		m.logger.Errorf("stopping driver: %s", err)
	}
	for name, registeredConnector := range m.drivers {
		if driver == registeredConnector {
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
			"ledger":    pipeline.Ledger,
			"connector": pipeline.ConnectorID,
		}),
	)

	// Detach the context as once the process of pipeline initialisation is started, we must not stop it
	ctx = context.WithoutCancel(ctx)

	m.logger.Infof("initializing connector")
	if err := m.initConnector(pipeline.ConnectorID); err != nil {
		return nil, fmt.Errorf("initializing connector: %w", err)
	}

	store, _, err := m.storage.OpenLedger(ctx, pipeline.Ledger)
	if err != nil {
		return nil, errors.Wrap(err, "opening ledger")
	}

	pipelineHandler := NewPipelineHandler(
		pipeline,
		store,
		m.drivers[pipeline.ConnectorID],
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

	m.logger.Infof("pipeline terminated, pruning connectors...")
	m.stopConnectorIfNeeded(ctx, handler)

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

func (m *Manager) stopConnectorIfNeeded(ctx context.Context, handler *PipelineHandler) {
	// Check if the connector associated to the pipeline is still in used
	connector := handler.connector
	for _, anotherPipeline := range m.pipelines {
		if anotherPipeline.connector == connector {
			// Connector still used, keep it
			return
		}
	}

	m.logger.Infof("connector %s no more used, stopping it...", handler.pipeline.ConnectorID)
	m.stopDriver(ctx, connector)
}

func (m *Manager) synchronizePipelines(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

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
		if handler := m.pipelines[pipeline.ID]; handler != nil {
			m.logger.Debugf("pipeline %s outdated, stopping it", pipeline.ID)
			if err := m.StopPipeline(ctx, pipeline.ID); err != nil {
				m.logger.Errorf("error stopping pipeline: %s", err)
				continue
			}
		}
		m.logger.Debugf("starting pipeline %s", pipeline.ID)
		if _, err := m.startPipeline(ctx, pipeline); err != nil {
			return err
		}
	}

l:
	for id := range m.pipelines {
		for _, pipeline := range pipelines {
			if id == pipeline.ID {
				continue l
			}
		}

		if err := m.StopPipeline(ctx, id); err != nil {
			m.logger.Errorf("error stopping pipeline: %s", err)
			continue
		}
	}

	return nil
}

func (m *Manager) Run(ctx context.Context) {
	if err := m.synchronizePipelines(ctx); err != nil {
		m.logger.Errorf("starting pipelines: %s", err)
	}

	signalChannel := <-m.stopChannel
	m.logger.Debugf("got stop signal")
	m.stopPipelines(ctx)
	m.pipelinesWaitGroup.Wait()
	close(signalChannel)
}

func (m *Manager) GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error) {
	pipeline, err := m.storage.GetPipeline(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
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

func (m *Manager) GetConnector(ctx context.Context, id string) (*ledger.Connector, error) {
	connector, err := m.storage.GetConnector(ctx, id)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, system.NewErrConnectorNotFound(id)
		default:
			return nil, err
		}
	}
	return connector, nil
}

func (m *Manager) ListConnectors(ctx context.Context) (*bunpaginate.Cursor[ledger.Connector], error) {
	return m.storage.ListConnectors(ctx)
}

func (m *Manager) DeleteConnector(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	driver, ok := m.drivers[id]
	if ok {
		for id, config := range m.pipelines {
			if config.pipeline.ConnectorID == id {
				if err := m.stopPipeline(ctx, id); err != nil {
					return fmt.Errorf("stopping pipeline: %w", err)
				}
			}
		}

		m.stopDriver(ctx, driver)
	}

	if err := m.storage.DeleteConnector(ctx, id); err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return system.NewErrConnectorNotFound(id)
		default:
			return err
		}
	}
	return nil
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
		return nil, err
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
		if errors.Is(err, sql.ErrNoRows) {
			return ledger.NewErrPipelineNotFound(id)
		}
		return err
	}

	return nil
}

func (m *Manager) ResetPipeline(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.stopPipeline(ctx, id); err != nil {
		return fmt.Errorf("stopping pipeline: %w", err)
	}

	pipeline, err := m.storage.UpdatePipeline(ctx, id, map[string]any{
		"enabled":     true,
		"last_log_id": nil,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ledger.NewErrPipelineNotFound(id)
		}
		return fmt.Errorf("updating pipeline: %w", err)
	}

	if _, err := m.startPipeline(ctx, *pipeline); err != nil {
		return fmt.Errorf("starting pipeline: %w", err)
	}
	return nil
}

func NewManager(
	storageDriver Storage,
	connectorFactory drivers.Factory,
	logger logging.Logger,
	connectorsConfigValidator ConfigValidator,
	options ...Option,
) *Manager {
	ret := &Manager{
		storage:                   storageDriver,
		stopChannel:               make(chan chan error, 1),
		pipelines:                 map[string]*PipelineHandler{},
		driverFactory:             connectorFactory,
		drivers:                   map[string]*DriverFacade{},
		logger:                    logger.WithField("component", "manager"),
		connectorsConfigValidator: connectorsConfigValidator,
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

var defaultOptions = []Option{}
