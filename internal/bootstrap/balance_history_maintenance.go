package bootstrap

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

type balanceHistoryCompactFunc func(context.Context, int) (bool, error)
type balanceHistoryChangesFunc func() <-chan struct{}

// balanceHistoryMaintenanceWorker performs bounded local logical-segment
// compaction. Projection archiving is intentionally out of scope: the audit is
// authoritative and the local peer store is rebuildable.
type balanceHistoryMaintenanceWorker struct {
	logger                logging.Logger
	maintenanceInterval   time.Duration
	compactionThreshold   int
	maxCompactionsPerPass int
	compact               balanceHistoryCompactFunc
	changes               balanceHistoryChangesFunc

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func newBalanceHistoryMaintenanceWorker(
	logger logging.Logger,
	config BalanceHistoryConfig,
	compact balanceHistoryCompactFunc,
	changes balanceHistoryChangesFunc,
) *balanceHistoryMaintenanceWorker {
	return &balanceHistoryMaintenanceWorker{
		logger:                logger,
		maintenanceInterval:   config.MaintenanceInterval,
		compactionThreshold:   config.SegmentCompactionThreshold,
		maxCompactionsPerPass: config.MaxCompactionsPerPass,
		compact:               compact,
		changes:               changes,
	}
}

func (w *balanceHistoryMaintenanceWorker) Start() {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.cancel != nil {
		w.mu.Unlock()

		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	w.cancel, w.done = cancel, done
	w.mu.Unlock()
	go w.run(ctx, done)
}

func (w *balanceHistoryMaintenanceWorker) Stop() {
	if w == nil {
		return
	}
	w.mu.Lock()
	cancel, done := w.cancel, w.done
	if cancel == nil {
		w.mu.Unlock()

		return
	}
	cancel()
	w.mu.Unlock()
	<-done
	w.mu.Lock()
	if w.done == done {
		w.cancel, w.done = nil, nil
	}
	w.mu.Unlock()
}

func (w *balanceHistoryMaintenanceWorker) run(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(w.maintenanceInterval)
	defer ticker.Stop()
	for {
		// Subscribe before inspecting the store. A publication which races with
		// the no-work result then closes this channel, so it cannot be missed in
		// the gap between the compaction pass and the select below.
		var changed <-chan struct{}
		if w.changes != nil {
			changed = w.changes()
		}
		if w.runCompactions(ctx) {
			// The bounded pass ended while every attempted merge still found
			// work. Continue immediately: compaction's own change signal may have
			// fired before this loop could subscribe to the replacement channel.
			runtime.Gosched()

			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-changed:
		}
	}
}

// runCompactions reports whether the pass exhausted its configured budget
// while still making progress and should therefore be continued immediately.
func (w *balanceHistoryMaintenanceWorker) runCompactions(ctx context.Context) bool {
	for range w.maxCompactionsPerPass {
		compacted, err := w.compact(ctx, w.compactionThreshold)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				w.logger.Errorf("Historical-balance compaction pass failed: %v", err)
			}

			return false
		}
		if !compacted {
			return false
		}
	}

	return true
}
