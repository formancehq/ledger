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

	// Linearize readiness publication against leadership loss. Cancellation
	// happens before cancelAndWait takes this lock, so a loss that starts after
	// WaitForApplied succeeds but before publication still wins this race.
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
	// run closes done only after releasing the publication lock, so this joins
	// both the waiter and any readiness callback that linearized first.
	<-generation.done
}
