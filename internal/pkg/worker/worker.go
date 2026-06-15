package worker

import (
	"context"
	"time"

	"go.uber.org/fx"
)

// ContextFromStop returns a context.Context that is canceled when stop is closed.
func ContextFromStop(stop <-chan struct{}) context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-stop
		cancel()
	}()

	return ctx
}

// DrainChannel reads items from ch and calls process for each one until stop
// is closed. It is the standard loop for channel-based workers.
func DrainChannel[T any](stop <-chan struct{}, ch <-chan T, process func(T)) {
	for {
		select {
		case <-stop:
			return
		case req := <-ch:
			process(req)
		}
	}
}

// RunTicker calls fn at the given interval until stop is closed. The ticker is
// cleaned up when RunTicker returns.
func RunTicker(stop <-chan struct{}, interval time.Duration, fn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			fn()
		}
	}
}

// Lifecycle is the interface for components with a simple Start/Stop lifecycle.
type Lifecycle interface {
	Start()
	Stop()
}

// FxHook returns an fx.Hook that starts and stops a Lifecycle component.
// It eliminates the boilerplate of wrapping Start/Stop in OnStart/OnStop closures.
func FxHook(w Lifecycle) fx.Hook {
	return fx.Hook{
		OnStart: func(_ context.Context) error {
			w.Start()

			return nil
		},
		OnStop: func(_ context.Context) error {
			w.Stop()

			return nil
		},
	}
}

// Worker provides goroutine lifecycle management (start/stop) for background
// workers. It encapsulates the stopCh/doneCh pattern shared across all workers.
type Worker struct {
	stopCh chan struct{}
	doneCh chan struct{}
}

// New creates a new Worker with initialized channels.
func New() Worker {
	return Worker{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Run launches fn in a background goroutine. The goroutine signals completion
// by closing doneCh when fn returns. fn receives stopCh to monitor for shutdown.
func (w *Worker) Run(fn func(stop <-chan struct{})) {
	go func() {
		defer close(w.doneCh)

		fn(w.stopCh)
	}()
}

// RunCtx is the ctx-flavored variant of Run: the callback receives a
// context.Context that is cancelled when Stop() is called. Equivalent to
// w.Run(func(stop) { fn(ContextFromStop(stop)) }), factored so every worker
// uses the same derivation — no caller needs to invent its own ctx (and in
// particular no caller is tempted to bound a Raft propose with
// context.WithTimeout(context.Background(), X), which would shadow the
// stop signal and, for write-once paths like SingleDelete, can cause a
// timeout-driven retry to duplicate an already-applied entry).
func (w *Worker) RunCtx(fn func(ctx context.Context)) {
	w.Run(func(stop <-chan struct{}) {
		fn(ContextFromStop(stop))
	})
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (w *Worker) Stop() {
	close(w.stopCh)
	<-w.doneCh
}

// StopCh returns the stop channel for use in select statements within callbacks.
func (w *Worker) StopCh() <-chan struct{} {
	return w.stopCh
}
