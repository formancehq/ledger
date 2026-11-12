package membership

import (
	"context"
	"sync"
	"time"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

// Coordinator serializes Raft membership changes with cluster policy updates.
// Its lock is held until a proposed policy has applied.
type Coordinator struct {
	sync.Mutex

	waitForPriorMembership func(context.Context) error
	stop                   <-chan struct{}
	terminal               <-chan struct{}
}

// Configure connects the Raft barrier and lifecycle signals before the coordinator is used.
func (c *Coordinator) Configure(waitForPriorMembership func(context.Context) error, stop, terminal <-chan struct{}) {
	c.waitForPriorMembership = waitForPriorMembership
	c.stop = stop
	c.terminal = terminal
}

func (c *Coordinator) AcquireClusterPolicy(ctx context.Context) (func(*futures.Future[state.ApplyResult]), error) {
	for !c.TryLock() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	if c.waitForPriorMembership != nil {
		if err := c.waitForPriorMembership(ctx); err != nil {
			c.Unlock()

			return nil, err
		}
	}

	return c.releaseAfter, nil
}

func (c *Coordinator) releaseAfter(future *futures.Future[state.ApplyResult]) {
	if future == nil {
		c.Unlock()

		return
	}
	go func() {
		defer c.Unlock()
		waitCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			select {
			case <-c.stop:
				cancel()
			case <-c.terminal:
				cancel()
			case <-waitCtx.Done():
			}
		}()
		_, _ = future.Wait(waitCtx)
	}()
}
