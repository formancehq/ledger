package controller

import (
	"context"
	"database/sql"
	"github.com/formancehq/ledger/internal"
	"sync"

	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal/replication/runner"
	"github.com/pkg/errors"
)

type Controller struct {
	mu             sync.Mutex
	inUsePipelines collectionutils.Set[string]

	runner Runner
	store  Store
	logger logging.Logger
}

// PausePipeline can return following errors:
// * ErrPipelineNotFound
// * ErrInvalidStateSwitch
// * ErrInUsePipeline
func (ctrl *Controller) PausePipeline(ctx context.Context, id string) error {
	return ctrl.callAndWaitStateOnPipeline(ctx, id, Pipeline.Pause, ledger.StateLabelPause)
}

// ResumePipeline can return following errors:
// * ErrPipelineNotFound
// * ErrInvalidStateSwitch
// * ErrInUsePipeline
func (ctrl *Controller) ResumePipeline(ctx context.Context, id string) error {
	return ctrl.callAndWaitStateOnPipeline(ctx, id, Pipeline.Resume, ledger.StateLabelReady)
}

// ResetPipeline can return following errors:
// * ErrPipelineNotFound
// * ErrInUsePipeline
func (ctrl *Controller) ResetPipeline(ctx context.Context, id string) error {
	return ctrl.callAndWaitStateOnPipeline(ctx, id, Pipeline.Reset, ledger.StateLabelInit)
}

// StopPipeline can return following errors:
// * ErrPipelineNotFound
// * ErrInUsePipeline
func (ctrl *Controller) StopPipeline(ctx context.Context, id string) error {
	originalError := ctrl.callAndWaitStateOnPipeline(ctx, id, Pipeline.Stop, ledger.StateLabelStop)
	// The method Controller.callAndWaitStateOnPipeline can return ErrPipelineNotFound because
	// the pipeline is not running, but the pipeline can exist in database.
	// So, we check its existence and map error if relevant
	if originalError != nil {
		if errors.Is(originalError, ErrPipelineNotFound("")) {
			pipeline, err := ctrl.store.GetPipeline(ctx, id)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return originalError
				}
				return err
			}
			return runner.NewErrInvalidStateSwitch(id, pipeline.State.Label, ledger.StateLabelStop)
		}
		return originalError
	}
	return nil
}

// DeletePipeline can return following errors:
// * ErrPipelineNotFound
// * ErrInUsePipeline
// * ErrConnectorUsed
// The method will stop the pipeline if it is actually running,
// then it will delete the pair from database
func (ctrl *Controller) DeletePipeline(ctx context.Context, id string) error {
	return ctrl.withPipelineLocked(id, func() error {
		if p, ok := ctrl.runner.GetPipeline(id); ok {
			if err := p.Stop(); err != nil {
				return errors.Wrap(err, "stopping pipeline")
			}
		}

		if err := ctrl.store.DeletePipeline(ctx, id); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return runner.NewErrPipelineNotFound(id)
			}
			return err
		}
		return nil
	})
}

// StartPipeline can return following errors:
// * ErrPipelineNotFound
// * ErrAlreadyStarted
// * ErrInUsePipeline
func (ctrl *Controller) StartPipeline(ctx context.Context, id string) error {
	return ctrl.withPipelineLocked(id, func() error {
		pipeline, err := ctrl.store.GetPipeline(ctx, id)
		if err != nil {
			switch {
			case errors.Is(err, sql.ErrNoRows):
				return runner.NewErrPipelineNotFound(id)
			default:
				return err
			}
		}
		_, err = ctrl.runner.StartPipeline(ctx, *pipeline)
		return err
	})
}

func (ctrl *Controller) markPipelineInUse(id string) (func(), error) {
	ctrl.mu.Lock()
	if ctrl.inUsePipelines.Contains(id) {
		ctrl.mu.Unlock()
		return nil, NewErrInUsePipeline(id)
	}
	ctrl.inUsePipelines.Put(id)
	ctrl.mu.Unlock()

	return func() {
		ctrl.mu.Lock()
		ctrl.inUsePipelines.Remove(id)
		ctrl.mu.Unlock()
	}, nil
}

func (ctrl *Controller) withPipelineLocked(id string, fn func() error) error {
	release, err := ctrl.markPipelineInUse(id)
	if err != nil {
		return err
	}
	defer release()

	return fn()
}

func (ctrl *Controller) callAndWaitStateOnPipeline(
	ctx context.Context,
	id string,
	fn func(pipeline Pipeline) error,
	label ledger.PipelineStateLabel,
) error {
	return ctrl.withPipelineLocked(id, func() error {
		p, ok := ctrl.runner.GetPipeline(id)
		if !ok {
			return runner.NewErrPipelineNotFound(id)
		}
		stateListener, cancelStateListener := p.GetActiveState().Listen(func(state ledger.PipelineState) bool {
			return state.Label == label
		})
		defer cancelStateListener()

		if err := fn(p); err != nil {
			return err
		}

		select {
		case <-stateListener:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

func New(runner Runner, store Store, logger logging.Logger) *Controller {
	return &Controller{
		runner:         runner,
		store:          store,
		inUsePipelines: collectionutils.NewSet[string](),
		logger:         logger,
	}
}
