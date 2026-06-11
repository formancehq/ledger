package worker

import (
	"context"
	"errors"
	"sync"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/otlplogs"
)

// SingleTaskExecutor manages a single background task that can be interrupted.
// At most one task runs at a time. Calling Run while a task is already running
// panics — callers must Interrupt first. Interrupt cancels the context and
// waits for the goroutine to finish before returning.
type SingleTaskExecutor struct {
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	terminated chan struct{}
	logger     logging.Logger
	errChan    chan error
}

// Run starts fn in a background goroutine. Panics if a task is already running.
func (t *SingleTaskExecutor) Run(ctx context.Context, fn func(ctx context.Context) error) {
	select {
	case <-t.terminated:
		t.mu.Lock()
		t.terminated = make(chan struct{})
		t.ctx, t.cancel = context.WithCancel(ctx)
		errCh := t.errChan
		t.mu.Unlock()

		otlplogs.Go(func() {
			defer func() {
				t.cancel()
				close(t.terminated)
			}()

			err := fn(t.ctx)
			// Ignore errors if context was cancelled (graceful shutdown)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}

			if err != nil {
				select {
				case errCh <- err:
				default:
					t.logger.WithFields(map[string]any{
						"error": err,
					}).Errorf("Single task executor error channel is full")
				}
			}
		}, t.logger)
	default:
		panic("already running")
	}
}

// Interrupt cancels the running task and waits for it to finish.
// No-op if no task is running.
func (t *SingleTaskExecutor) Interrupt() {
	t.mu.Lock()
	terminated := t.terminated
	cancel := t.cancel
	t.mu.Unlock()

	select {
	case <-terminated:
	default:
		cancel()
		<-terminated
	}
}

// Error returns the long-lived channel that receives task errors.
func (t *SingleTaskExecutor) Error() chan error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.errChan
}

// NewSingleTaskExecutor creates a SingleTaskExecutor in the idle state.
func NewSingleTaskExecutor(logger logging.Logger) *SingleTaskExecutor {
	terminatedChan := make(chan struct{})
	close(terminatedChan)

	return &SingleTaskExecutor{
		terminated: terminatedChan,
		logger:     logger,
		errChan:    make(chan error, 1),
	}
}
