package raft

import (
	"context"
	"errors"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
)

// singleTaskExecutor manages a single background task that can be interrupted
type singleTaskExecutor struct {
	ctx        context.Context
	cancel     context.CancelFunc
	terminated chan struct{}
	logger     logging.Logger
}

func (t *singleTaskExecutor) run(ctx context.Context, fn func(ctx context.Context) error) {
	select {
	case <-t.terminated:
		t.terminated = make(chan struct{})
		t.ctx, t.cancel = context.WithCancel(ctx)

		otlplogs.Go(func() {
			defer func() {
				t.cancel()
				close(t.terminated)
			}()

			err := fn(t.ctx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				panic(err)
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

func newSingleTaskExecutor(logger logging.Logger) *singleTaskExecutor {
	terminatedChan := make(chan struct{})
	close(terminatedChan)
	return &singleTaskExecutor{
		terminated: terminatedChan,
		logger:     logger,
	}
}
