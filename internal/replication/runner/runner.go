package runner

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/pointer"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/leadership"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/pkg/errors"
)

type Runner struct {
	mu sync.Mutex

	stopChannel        chan chan error
	storage            Storage
	pipelines          map[string]*PipelineHandler
	pipelinesWaitGroup sync.WaitGroup
	logger             logging.Logger

	driverFactory drivers.Factory
	drivers       map[string]*DriverFacade
	syncPeriod    time.Duration

	pipelineOptions []PipelineOption
}

func (runner *Runner) initConnector(connectorID string) error {

	_, ok := runner.drivers[connectorID]
	if ok {
		return nil
	}

	driver, _, err := runner.driverFactory.Create(context.Background(), connectorID)
	if err != nil {
		return err
	}

	driverFacade := newDriverFacade(driver, runner.logger, 2*time.Second)
	driverFacade.Run(context.Background())

	runner.drivers[connectorID] = driverFacade

	return nil
}

func (runner *Runner) stopConnector(ctx context.Context, connector drivers.Driver) {
	if err := connector.Stop(ctx); err != nil {
		runner.logger.Errorf("stopping connector: %s", err)
	}
	for name, registeredConnector := range runner.drivers {
		if connector == registeredConnector {
			delete(runner.drivers, name)
			return
		}
	}
}

func (runner *Runner) StartPipeline(ctx context.Context, pipeline ledger.Pipeline) (*PipelineHandler, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	_, ok := runner.pipelines[pipeline.ID]
	if ok {
		return nil, ledger.NewErrAlreadyStarted(pipeline.ID)
	}

	ctx = logging.ContextWithLogger(
		ctx,
		runner.logger.WithFields(map[string]any{
			"module":    pipeline.Ledger,
			"connector": pipeline.ConnectorID,
		}),
	)

	// Detach the context as once the process of pipeline initialisation is started, we must not stop it
	ctx = context.WithoutCancel(ctx)

	runner.logger.Infof("initializing pipeline")
	if err := runner.initConnector(pipeline.ConnectorID); err != nil {
		return nil, err
	}

	store, _, err := runner.storage.OpenLedger(ctx, pipeline.Ledger)
	if err != nil {
		return nil, errors.Wrap(err, "opening ledger")
	}

	if pipeline.LastLogID == nil {
		pipeline.LastLogID = pointer.For(0)
	}

	pipelineHandler := NewPipelineHandler(
		pipeline,
		store,
		runner.drivers[pipeline.ConnectorID],
		runner.logger,
		runner.pipelineOptions...,
	)
	runner.pipelines[pipeline.ID] = pipelineHandler
	runner.pipelinesWaitGroup.Add(1)

	// ignore the cancel function, as it will be called by the pipeline at its end
	subscription := make(chan int)

	go func() {
		for lastLogID := range subscription {
			if err := runner.storage.StorePipelineState(ctx, pipeline.ID, lastLogID); err != nil {
				runner.logger.Errorf("Unable to store state: %s", err)
			}
		}
	}()
	go func() {
		defer func() {
			runner.mu.Lock()
			defer runner.mu.Unlock()
			defer runner.pipelinesWaitGroup.Done()
			close(subscription)

			delete(runner.pipelines, pipeline.ID)
			runner.logger.Infof("pipeline terminated, pruning connectors...")
			runner.stopConnectorIfNeeded(ctx, pipelineHandler)
		}()
		pipelineHandler.Run(ctx, subscription)
	}()

	return pipelineHandler, nil
}

func (runner *Runner) stopPipeline(ctx context.Context, id string) error {
	if err := runner.pipelines[id].Shutdown(ctx); err != nil {
		return fmt.Errorf("error stopping pipeline: %w", err)
	}

	delete(runner.pipelines, id)
	return nil
}

func (runner *Runner) stopPipelines(ctx context.Context) {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	for id := range runner.pipelines {
		if err := runner.stopPipeline(ctx, id); err != nil {
			runner.logger.Errorf("error stopping pipeline: %s", err)
		}
	}
}

func (runner *Runner) stopConnectorIfNeeded(ctx context.Context, handler *PipelineHandler) {
	// Check if the connector associated to the pipeline is still in used
	connector := handler.connector
	for _, anotherPipeline := range runner.pipelines {
		if anotherPipeline.connector == connector {
			// Connector still used, keep it
			return
		}
	}

	runner.logger.Infof("connector %s no more used, stopping it...", handler.pipeline.ConnectorID)
	runner.stopConnector(ctx, connector)
}

func (runner *Runner) synchronizePipelines(ctx context.Context) error {
	runner.logger.Debug("restore pipelines from store")
	pipelines, err := runner.storage.ListEnabledPipelines(ctx)
	if err != nil {
		return fmt.Errorf("reading pipelines from store: %w", err)
	}

	for _, pipeline := range pipelines {
		if existingPipeline := runner.GetPipeline(pipeline.ID); existingPipeline != nil {
			if pipeline.LastLogID != nil || existingPipeline.pipeline.LastLogID == nil || *existingPipeline.pipeline.LastLogID == 0 {
				continue
			}

			if err := runner.stopPipeline(ctx, pipeline.ID); err != nil {
				runner.logger.Errorf("error stopping pipeline: %s", err)
				continue
			}
		}
		if _, err := runner.StartPipeline(ctx, pipeline); err != nil {
			return err
		}
	}

l:
	for id := range runner.pipelines {
		for _, pipeline := range pipelines {
			if id == pipeline.ID {
				continue l
			}
		}

		if err := runner.stopPipeline(ctx, id); err != nil {
			runner.logger.Errorf("error stopping pipeline: %s", err)
			continue
		}
	}

	return nil
}

// todo: use db handle
func (runner *Runner) Run(ctx context.Context, db *leadership.DatabaseHandle) {
	if err := runner.synchronizePipelines(ctx); err != nil {
		runner.logger.Errorf("starting pipelines: %s", err)
	}

	for {
		select {
		case signalChannel := <-runner.stopChannel:
			runner.logger.Debugf("got stop signal")
			runner.stopPipelines(ctx)
			runner.pipelinesWaitGroup.Wait()
			close(signalChannel)
			return
		case <-time.After(runner.syncPeriod):
			if err := runner.synchronizePipelines(ctx); err != nil {
				runner.logger.Errorf("starting pipelines: %s", err)
			}
		}
	}
}

func (runner *Runner) GetPipeline(id string) *PipelineHandler {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	for p, pipeline := range runner.pipelines {
		if id == p {
			return pipeline
		}
	}

	return nil
}

func (runner *Runner) Stop(ctx context.Context) error {
	runner.logger.Info("stopping runner")
	signalChannel := make(chan error, 1)

	select {
	case runner.stopChannel <- signalChannel:
		runner.logger.Debug("stopping runner signal sent")
		select {
		case <-signalChannel:
			runner.logger.Info("runner stopped")
			return nil
		case <-ctx.Done():
			runner.logger.Error("context canceled while waiting for runner termination")
			return ctx.Err()
		}
	case <-ctx.Done():
		runner.logger.Error("context canceled while waiting for runner signal handling")
		return ctx.Err()
	}
}

func (runner *Runner) GetConnector(name string) *DriverFacade {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	return runner.drivers[name]
}

func NewRunner(
	storageDriver Storage,
	connectorFactory drivers.Factory,
	logger logging.Logger,
	options ...Option,
) *Runner {
	ret := &Runner{
		storage:       storageDriver,
		stopChannel:   make(chan chan error, 1),
		pipelines:     map[string]*PipelineHandler{},
		driverFactory: connectorFactory,
		drivers:       map[string]*DriverFacade{},
		logger:        logger.WithField("component", "runner"),
	}

	for _, option := range append(defaultOptions, options...) {
		option(ret)
	}

	return ret
}

type Option func(r *Runner)

func WithSyncPeriod(duration time.Duration) Option {
	return func(r *Runner) {
		r.syncPeriod = duration
	}
}

func WithPipelineOptions(options ...PipelineOption) Option {
	return func(r *Runner) {
		r.pipelineOptions = append(r.pipelineOptions, options...)
	}
}

var defaultOptions = []Option{
	WithSyncPeriod(5 * time.Second),
}
