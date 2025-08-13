package contextutil

import (
	"context"
	"time"
)

type detachedContext struct {
	parent context.Context
}

var _ context.Context = (*detachedContext)(nil)

func (c *detachedContext) Done() <-chan struct{} {
	return nil
}

func (c *detachedContext) Deadline() (deadline time.Time, ok bool) {
	return c.parent.Deadline()
}

func (c *detachedContext) Err() error {
	return c.parent.Err()
}

func (c *detachedContext) Value(key interface{}) interface{} {
	return c.parent.Value(key)
}

func Detached(parent context.Context) (context.Context, context.CancelFunc) {
	c := &detachedContext{parent: parent}
	if deadline, ok := parent.Deadline(); ok {
		return context.WithDeadline(c, deadline)
	}
	return context.WithCancel(c)
}

func DetachedWithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(&detachedContext{parent: parent}, timeout)
}
