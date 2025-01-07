package runner

import (
	"context"

	"github.com/formancehq/go-libs/v2/logging"
)

type StateHandler struct {
	fn                 func(ctx context.Context, ready chan struct{}) error
	stateContext       context.Context
	cancelStateContext context.CancelFunc
	doneChan           chan struct{}
	err                error
	logger             logging.Logger
}

func (h *StateHandler) Run(ctx context.Context, ready chan struct{}) {
	h.stateContext, h.cancelStateContext = context.WithCancel(ctx)
	h.doneChan = make(chan struct{})
	go func() {
		h.err = h.fn(h.stateContext, ready)
		if h.err != nil {
			h.logger.Errorf("unexpected state termination: %s", h.Err())
		}
		close(h.doneChan)
	}()
}

func (h *StateHandler) Switch(ctx context.Context, fn func(ctx context.Context, ready chan struct{}) error) error {
	select {
	case <-h.doneChan:
		// If a state was handled, it is terminated
	default:
		// Cancel active task
		if err := h.Cancel(ctx); err != nil {
			return err
		}
	}

	h.fn = fn
	ready := make(chan struct{})
	h.Run(ctx, ready)

	select {
	case <-ready:
		return nil
	case <-h.doneChan:
		return h.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *StateHandler) Cancel(ctx context.Context) error {
	if h.cancelStateContext != nil {
		h.cancelStateContext()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.doneChan: // Wait for the end of the state handling
			if h.err != nil {
				return h.err
			}
		}
		h.cancelStateContext = nil
	}
	return nil
}

func (h *StateHandler) Err() error {
	return h.err
}

func NewStateHandler(logger logging.Logger, fn func(ctx context.Context, ready chan struct{}) error) *StateHandler {
	return &StateHandler{
		fn:     fn,
		logger: logger,
	}
}

func NewEmptyStateHandler(logger logging.Logger) *StateHandler {
	return NewStateHandler(logger, func(ctx context.Context, ready chan struct{}) error {
		return nil
	})
}
