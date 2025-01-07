package runner

import (
	"context"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/query"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"sync"
	"time"

	ingester "github.com/formancehq/ledger/internal/replication"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"
)

var (
	DefaultPullRetryPeriod    = 10 * time.Second
	DefaultPushRetryPeriod    = 10 * time.Second
	DefaultStateRetryInterval = 5 * time.Second
)

type PipelineHandlerConfig struct {
	ModulePullRetryPeriod    time.Duration
	ConnectorPushRetryPeriod time.Duration
	StateRetryInterval       time.Duration
}

type PipelineOption func(config *PipelineHandlerConfig)

func WithModulePullPeriod(v time.Duration) PipelineOption {
	return func(config *PipelineHandlerConfig) {
		config.ModulePullRetryPeriod = v
	}
}

func WithConnectorPushRetryPeriod(v time.Duration) PipelineOption {
	return func(config *PipelineHandlerConfig) {
		config.ConnectorPushRetryPeriod = v
	}
}

func WithStateRetryInterval(v time.Duration) PipelineOption {
	return func(config *PipelineHandlerConfig) {
		config.StateRetryInterval = v
	}
}

var (
	defaultPipelineOptions = []PipelineOption{
		WithModulePullPeriod(DefaultPullRetryPeriod),
		WithStateRetryInterval(DefaultStateRetryInterval),
		WithConnectorPushRetryPeriod(DefaultPushRetryPeriod),
	}
)

type PipelineHandler struct {
	mu sync.Mutex

	pipeline       ingester.Pipeline
	stopChannel    chan chan error
	store          LogFetcher
	connector      drivers.Driver
	expectedState  *Signal[ingester.State]
	activeState    *Signal[ingester.State]
	pipelineConfig PipelineHandlerConfig
	stateHandler   *StateHandler
	logger         logging.Logger
}

// Pause can return following errors:
// * ErrAlreadyPaused
func (p *PipelineHandler) Pause() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	actualExpectedState := p.expectedState.Actual()
	if actualExpectedState.Label == ingester.StateLabelPause {
		return NewErrInvalidStateSwitch(p.pipeline.ID, actualExpectedState.Label, ingester.StateLabelStop)
	}
	p.expectedState.Signal(ingester.NewPauseState(
		*p.activeState.Actual(),
	))

	return nil
}

// Resume can return following errors:
// * ErrNotInPauseState
func (p *PipelineHandler) Resume() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	actualExpectedState := p.expectedState.Actual()
	if actualExpectedState.Label != ingester.StateLabelPause {
		return NewErrInvalidStateSwitch(p.pipeline.ID, actualExpectedState.Label, ingester.StateLabelPause)
	}
	p.expectedState.Signal(*actualExpectedState.PreviousState)

	return nil
}

func (p *PipelineHandler) Reset() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.expectedState.Signal(ingester.NewReadyState())

	return nil
}

// Stop try to stop the pipeline
// It is asynchronous and can controller by watching the active state of the pipeline
// see GetActiveState
func (p *PipelineHandler) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	actualExpectedState := p.expectedState.Actual()
	if actualExpectedState.Label == ingester.StateLabelStop {
		return NewErrInvalidStateSwitch(p.pipeline.ID, actualExpectedState.Label, ingester.StateLabelStop)
	}

	p.expectedState.Signal(ingester.NewStopState(*actualExpectedState))

	return nil
}

func (p *PipelineHandler) switchToState(ctx context.Context, newState ingester.State) bool {
	if p.activeState.Actual() != nil &&
		newState.Label == p.activeState.Actual().Label {
		return true
	}
	p.logger.Infof("Switching to state '%s'", newState.Label)

	var fn func(ctx context.Context, readyChan chan struct{}) error
	switch newState.Label {
	case ingester.StateLabelInit:
		fn = p.run
	case ingester.StateLabelReady:
		fn = p.run
	case ingester.StateLabelPause:
		fn = p.pause
	case ingester.StateLabelStop:
		fn = p.stop
	}
	if err := p.stateHandler.Switch(ctx, fn); err != nil {
		p.logger.Errorf("Error switching to state '%s': %s", newState.Label, err)
		return true
	}
	p.activeState.Signal(newState)
	p.logger.Infof("Switched to state '%s'", newState.Label)

	return newState.Label != ingester.StateLabelStop
}

func (p *PipelineHandler) Run(ctx context.Context) {
	defer p.activeState.Close()

	stateChangedListener, cancelExpectedStateListener := p.expectedState.Listen()
	defer cancelExpectedStateListener()

	p.stateHandler = NewEmptyStateHandler(p.logger)
	p.stateHandler.Run(ctx, make(chan struct{}))

	for {
		select {
		case newState := <-stateChangedListener:
			if !p.switchToState(ctx, newState) {
				return
			}
		case errorChannel := <-p.stopChannel:
			p.logger.Debugf("stopping pipeline signal received...")
			err := p.stateHandler.Cancel(ctx)
			errorChannel <- err
			if err != nil {
				p.logger.Errorf("pipeline stopped with error: %s", err)
			} else {
				p.logger.Infof("pipeline stopped")
			}
			return
		case <-time.After(p.pipelineConfig.StateRetryInterval):
			if !p.switchToState(ctx, *p.expectedState.Actual()) {
				return
			}
		}
	}
}

func (p *PipelineHandler) run(ctx context.Context, ready chan struct{}) error {
	close(ready)

	p.activeState.Signal(ingester.NewReadyState())

	wg := sync.WaitGroup{}
	lastID := p.expectedState.Actual().LastID
	for {
		logs, err := p.store.ListLogs(ctx, ledgercontroller.ColumnPaginatedQuery[any]{
			PageSize: 100,
			Column:   "id",
			Options: ledgercontroller.ResourceQuery[any]{
				Builder: query.Gte("id", lastID),
			},
			Order: pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
		})
		if err != nil {
			p.logger.Errorf("Error fetching logs: %s", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(p.pipelineConfig.ModulePullRetryPeriod):
				continue
			}
		}

		wg.Add(len(logs.Data))
		for _, log := range logs.Data {
			go func() {
				defer wg.Done()
				for {
					itemsErrors, err := p.connector.Accept(ctx, ingester.LogWithLedger{
						Log:    log,
						Ledger: p.pipeline.Ledger,
					})
					if err == nil {
						err = itemsErrors[0]
					}
					if err != nil {
						p.logger.Errorf("Error pushing data on connector: %s", err)
						select {
						case <-ctx.Done():
							return
						case <-time.After(p.pipelineConfig.ConnectorPushRetryPeriod):
							continue
						}
					}
					break
				}
			}()
		}

		wg.Wait()

		if !logs.HasMore {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(p.pipelineConfig.ModulePullRetryPeriod):
				continue
			}
		}
		lastID = logs.Data[len(logs.Data)-1].ID

		p.activeState.Signal(ingester.NewReadyStateWithID(lastID))
	}
}

func (p *PipelineHandler) pause(ctx context.Context, ready chan struct{}) error {
	close(ready)

	select {
	case <-ctx.Done():
		return nil
	}
}

func (p *PipelineHandler) stop(ctx context.Context, ready chan struct{}) error {
	close(ready)

	select {
	case <-ctx.Done():
		return nil
	}
}

func (p *PipelineHandler) Shutdown(ctx context.Context) error {
	p.logger.Infof("shutdowning pipeline")
	errorChannel := make(chan error, 1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.stopChannel <- errorChannel:
		p.logger.Debugf("shutdowning pipeline signal sent")
		select {
		case err := <-errorChannel:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *PipelineHandler) GetActiveState() *Signal[ingester.State] {
	if p == nil {
		return nil
	}
	return p.activeState
}

func NewPipelineHandler(
	pipeline ingester.Pipeline,
	store LogFetcher,
	connector drivers.Driver,
	logger logging.Logger,
	opts ...PipelineOption,
) *PipelineHandler {
	config := PipelineHandlerConfig{}
	for _, opt := range append(defaultPipelineOptions, opts...) {
		opt(&config)
	}

	return &PipelineHandler{
		pipeline:       pipeline,
		stopChannel:    make(chan chan error, 1),
		store:          store,
		connector:      connector,
		expectedState:  NewSignal(&pipeline.State),
		activeState:    NewSignal[ingester.State](nil),
		pipelineConfig: config,
		logger: logger.
			WithField("component", "pipeline").
			WithField("module", pipeline.Ledger).
			WithField("connector", pipeline.ConnectorID),
	}
}
