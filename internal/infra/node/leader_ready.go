package node

import (
	"context"
	"sync"
)

type leaderReadyGeneration struct {
	ready  chan struct{}
	lost   chan struct{}
	done   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
}

func newLeaderReadyGeneration(parent context.Context) *leaderReadyGeneration {
	ctx, cancel := context.WithCancel(parent)

	return &leaderReadyGeneration{
		ready:  make(chan struct{}),
		lost:   make(chan struct{}),
		done:   make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}
}

func newCompletedLeaderReadyGeneration() *leaderReadyGeneration {
	generation := newLeaderReadyGeneration(context.Background())
	close(generation.ready)
	close(generation.done)

	return generation
}

func (generation *leaderReadyGeneration) run(
	wait func(context.Context) error,
	emit func(),
) error {
	defer close(generation.done)

	if err := wait(generation.ctx); err != nil {
		return err
	}

	// Recheck cancellation after the FSM wait, before publishing readiness.
	// cancelAndWait joins done, ensuring publication and its synchronous
	// callback finish before the leadership-loss transition is published.
	generation.mu.Lock()
	defer generation.mu.Unlock()

	if err := generation.ctx.Err(); err != nil {
		return err
	}

	emit()
	close(generation.ready)

	return nil
}

func (generation *leaderReadyGeneration) cancelAndWait() {
	generation.cancel()
	close(generation.lost)
	// run closes done only after publication and its synchronous callback
	// finish. This join orders them before the leadership-loss transition;
	// cancelAndWait does not acquire the publication lock.
	<-generation.done
}
