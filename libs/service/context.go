package service

import (
	"context"
)

type contextKey string

const (
	lifecycleContextKey contextKey = "ready"
)

var closedChan = make(chan struct{})

func init() {
	close(closedChan)
}

type lifecycle struct {
	ready   chan struct{}
	stopped chan struct{}
}

func newLifecycle() *lifecycle {
	return &lifecycle{
		ready:   make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

func contextWithLifecycle(ctx context.Context, lc *lifecycle) context.Context {
	return context.WithValue(ctx, lifecycleContextKey, lc)
}

func lifecycleFromContext(ctx context.Context) *lifecycle {
	lc := ctx.Value(lifecycleContextKey)
	if lc == nil {
		return nil
	}
	return lc.(*lifecycle)
}

func ContextWithLifecycle(ctx context.Context) context.Context {
	return context.WithValue(ctx, lifecycleContextKey, newLifecycle())
}

func markAsAppReady(ctx context.Context) {
	lc := lifecycleFromContext(ctx)
	if lc == nil {
		return
	}
	close(lc.ready)
}

func markAsAppStopped(ctx context.Context) {
	lc := lifecycleFromContext(ctx)
	if lc == nil {
		return
	}
	close(lc.stopped)
}

func Ready(ctx context.Context) chan struct{} {
	lc := lifecycleFromContext(ctx)
	if lc == nil {
		return closedChan
	}
	return lc.ready
}

func Stopped(ctx context.Context) chan struct{} {
	lc := lifecycleFromContext(ctx)
	if lc == nil {
		return closedChan
	}

	return lc.stopped
}
