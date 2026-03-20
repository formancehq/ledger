package node

import (
	"context"
	"errors"
	"sync"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/otlplogs"
)

// singleTaskExecutor manages a single background task that can be interrupted.
type singleTaskExecutor struct {
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	terminated chan struct{}
	logger     logging.Logger
	errChan    chan error
}

func (t *singleTaskExecutor) run(ctx context.Context, fn func(ctx context.Context) error) {
	select {
	case <-t.terminated:
		t.mu.Lock()
		t.terminated = make(chan struct{})
		t.ctx, t.cancel = context.WithCancel(ctx)
		errCh := make(chan error, 1)
		t.errChan = errCh
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
				errCh <- err
			}
		}, t.logger)
	default:
		panic("already running")
	}
}

func (t *singleTaskExecutor) interrupt() {
	select {
	case <-t.terminated:
	default:
		t.cancel()
		<-t.terminated
	}
}

func (t *singleTaskExecutor) error() chan error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.errChan
}

func newSingleTaskExecutor(logger logging.Logger) *singleTaskExecutor {
	terminatedChan := make(chan struct{})
	close(terminatedChan)

	return &singleTaskExecutor{
		terminated: terminatedChan,
		logger:     logger,
	}
}
