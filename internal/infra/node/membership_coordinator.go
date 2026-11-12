package node

import (
	"context"
	"sync"
	"time"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

// MembershipCoordinator serializes Raft membership changes with policy activation.
// Its lock is held until an activating policy has applied.
type MembershipCoordinator struct {
	sync.Mutex

	waitForPriorMembership func(context.Context) error
	stop                   <-chan struct{}
	terminal               <-chan struct{}
}

func (c *MembershipCoordinator) AcquireCapabilityMembership(ctx context.Context) (func(*futures.Future[state.ApplyResult]), error) {
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

func (c *MembershipCoordinator) releaseAfter(future *futures.Future[state.ApplyResult]) {
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
