// Package tailworker provides the shared skeleton for background workers that
// tail an append-only sequence: a Start/Stop lifecycle around a ticker loop
// with an optional wake signal, plus a helper for the standard progress/lag
// OTEL gauges. It carries no storage dependency (see readstore.Uint64Cursor
// for the persisted progress cursor).
package tailworker

import (
	"context"
	"errors"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/worker"
)

// Config parameterises a TailWorker.
type Config struct {
	// Name identifies the worker in log messages.
	Name string
	// Logger is used for boot and steady-state error logging.
	Logger logging.Logger
	// Ticker is the steady-state polling interval.
	Ticker time.Duration
	// Wake, when non-nil, triggers an extra Tick as soon as it receives.
	// A nil channel is never selectable, so leaving it nil yields a
	// pure-ticker loop with no special-casing.
	Wake <-chan struct{}
	// Boot runs once before the ticker starts. A non-nil error aborts the
	// loop (the worker does no further work). Optional.
	Boot func(context.Context) error
	// Tick runs once per ticker fire and per Wake signal. A context.Canceled
	// error is swallowed (expected on shutdown); any other error is logged
	// and the loop continues.
	Tick func(context.Context) error
}

// TailWorker runs Config.Tick on a ticker (and optional wake) until Stop.
type TailWorker struct {
	cfg Config
	w   worker.Worker
}

// New constructs a TailWorker. It does not start any goroutine.
func New(cfg Config) *TailWorker {
	return &TailWorker{cfg: cfg}
}

// Start launches the background loop.
func (t *TailWorker) Start() {
	t.w = worker.New()
	t.w.RunCtx(t.loop)
}

// Stop signals the loop to stop and waits for it to finish. It must only be
// called after Start (mirrors worker.Worker): calling it on a TailWorker that
// was never started panics on a nil channel close. Consumers that start
// conditionally must guard Stop with the same condition.
func (t *TailWorker) Stop() {
	t.w.Stop()
}

func (t *TailWorker) loop(ctx context.Context) {
	if t.cfg.Boot != nil {
		if err := t.cfg.Boot(ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				t.cfg.Logger.Errorf("%s boot: %v", t.cfg.Name, err)
			}

			return
		}
	}

	ticker := time.NewTicker(t.cfg.Ticker)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-t.cfg.Wake:
		}

		if err := t.cfg.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.cfg.Logger.Errorf("%s tick: %v", t.cfg.Name, err)
		}
	}
}
