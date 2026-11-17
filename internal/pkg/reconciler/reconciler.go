// Package reconciler provides a small periodic reconciliation loop with an
// explicit trigger for events that should be handled before the next tick.
package reconciler

import (
	"context"
	"time"

	"github.com/formancehq/ledger/v3/internal/pkg/worker"
)

// Reconciler invokes reconcile immediately when started, periodically at
// interval, and whenever Trigger is called. Trigger notifications are
// coalesced so bursts of events cannot grow an unbounded queue.
type Reconciler struct {
	reconcile func(context.Context)
	interval  time.Duration
	trigger   chan struct{}
	w         worker.Worker
}

// New creates a periodic reconciler. interval must be positive.
func New(reconcile func(context.Context), interval time.Duration) *Reconciler {
	if interval <= 0 {
		panic("reconciler interval must be positive")
	}

	return &Reconciler{
		reconcile: reconcile,
		interval:  interval,
		trigger:   make(chan struct{}, 1),
		w:         worker.New(),
	}
}

// Trigger asks the running reconciler to converge immediately. The signal is
// coalesced because several events may be committed in one batch.
func (r *Reconciler) Trigger() {
	select {
	case r.trigger <- struct{}{}:
	default:
	}
}

// Start runs an immediate reconciliation, then one per interval, until Stop.
func (r *Reconciler) Start() {
	r.w.Run(func(stop <-chan struct{}) {
		ctx := worker.ContextFromStop(stop)
		r.reconcile(ctx)
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				r.reconcile(ctx)
			case <-r.trigger:
				r.reconcile(ctx)
			}
		}
	})
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (r *Reconciler) Stop() {
	r.w.Stop()
}
