package runner

import (
	"context"
	ledger "github.com/formancehq/ledger/internal"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/pkg/errors"
)

type Runner struct {
	mu sync.Mutex

	readyChannel       chan struct{}
	stopChannel        chan chan error
	storageDriver      StorageDriver
	systemStore        SystemStore
	pipelines          map[string]*PipelineHandler
	pipelinesWaitGroup sync.WaitGroup
	logger             logging.Logger

	driverFactory drivers.Factory
	drivers       map[string]*DriverFacade
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
		return nil, NewErrAlreadyStarted(pipeline.ID)
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

	if pipeline.State.Label == ledger.StateLabelStop {
		pipeline.State = *pipeline.State.PreviousState
	}

	store, _, err := runner.storageDriver.OpenLedger(ctx, pipeline.Ledger)
	if err != nil {
		return nil, errors.Wrap(err, "opening ledger")
	}

	pipelineHandler := NewPipelineHandler(
		pipeline,
		store,
		runner.drivers[pipeline.ConnectorID],
		runner.logger,
	)
	runner.pipelines[pipeline.ID] = pipelineHandler
	runner.pipelinesWaitGroup.Add(1)

	// ignore the cancel function, as it will be called by the pipeline at its end
	subscription, _ := pipelineHandler.GetActiveState().Listen()

	go runner.listenSubscription(ctx, pipeline, subscription)()
	go func() {
		defer func() {
			runner.mu.Lock()
			defer runner.mu.Unlock()
			defer runner.pipelinesWaitGroup.Done()

			delete(runner.pipelines, pipeline.ID)
			runner.logger.Infof("pipeline terminated, pruning connectors...")
			runner.stopConnectorIfNeeded(ctx, pipelineHandler)
		}()
		pipelineHandler.Run(ctx)
	}()

	return pipelineHandler, nil
}

func (runner *Runner) listenSubscription(ctx context.Context, pipeline ledger.Pipeline, subscription <-chan ledger.PipelineState) func() {
	return func() {
		for state := range subscription {
			if err := runner.systemStore.StorePipelineState(ctx, pipeline.ID, state); err != nil {
				runner.logger.Errorf("Unable to store state: %s", err)
			}
		}
	}
}

func (runner *Runner) stopPipelines(ctx context.Context) {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	for _, pipeline := range runner.pipelines {
		if err := pipeline.Shutdown(ctx); err != nil {
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

func (runner *Runner) Run(ctx context.Context) error {
	runner.logger.Info("starting")
	close(runner.readyChannel)

	runner.logger.Info("Waiting stop or error")
	signalChannel := <-runner.stopChannel
	runner.logger.Debugf("got stop signal")
	runner.stopPipelines(ctx)
	runner.pipelinesWaitGroup.Wait()
	close(signalChannel)

	return nil
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

func (runner *Runner) Ready() chan struct{} {
	return runner.readyChannel
}

func (runner *Runner) IsReady() bool {
	select {
	case <-runner.Ready():
		return true
	default:
		return false
	}
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

func (runner *Runner) StartAsync(ctx context.Context) error {
	go func() {
		// notes(gfyrag): detach the context from the provided context to allow to properly close the runner instead of relying
		// on the context cancellation
		if err := runner.Run(context.Background()); err != nil {
			panic(err)
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-runner.Ready():
		return nil
	}
}

func (runner *Runner) GetConnector(name string) *DriverFacade {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	return runner.drivers[name]
}

func NewRunner(
	storageDriver StorageDriver,
	systemStore SystemStore,
	connectorFactory drivers.Factory,
	logger logging.Logger,
) *Runner {
	return &Runner{
		storageDriver: storageDriver,
		systemStore:   systemStore,
		stopChannel:   make(chan chan error, 1),
		pipelines:     map[string]*PipelineHandler{},
		driverFactory: connectorFactory,
		readyChannel:  make(chan struct{}),
		drivers:       map[string]*DriverFacade{},
		logger:        logger.WithField("component", "runner"),
	}
}

func RestorePipelines(ctx context.Context, store SystemStore, runner *Runner) error {
	runner.logger.Info("restore states from store")
	states, err := store.ListEnabledPipelines(ctx)
	if err != nil {
		return errors.Wrap(err, "reading states from store")
	}

	for _, state := range states {
		if _, err := runner.StartPipeline(ctx, state); err != nil {
			return err
		}
	}

	return nil
}
