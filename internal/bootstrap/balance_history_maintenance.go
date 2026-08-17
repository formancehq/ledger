package bootstrap

import (
	"context"
	"errors"
	"sync"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

type balanceHistoryCompactFunc func(context.Context, int) (bool, error)

// balanceHistoryMaintenanceWorker performs bounded local logical-segment
// compaction. Projection archiving is intentionally out of scope: the audit is
// authoritative and the local peer store is rebuildable.
type balanceHistoryMaintenanceWorker struct {
	logger                logging.Logger
	maintenanceInterval   time.Duration
	compactionThreshold   int
	maxCompactionsPerPass int
	compact               balanceHistoryCompactFunc

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func newBalanceHistoryMaintenanceWorker(logger logging.Logger, config BalanceHistoryConfig, compact balanceHistoryCompactFunc) *balanceHistoryMaintenanceWorker {
	return &balanceHistoryMaintenanceWorker{
		logger:                logger,
		maintenanceInterval:   config.MaintenanceInterval,
		compactionThreshold:   config.SegmentCompactionThreshold,
		maxCompactionsPerPass: config.MaxCompactionsPerPass,
		compact:               compact,
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
	w.runCompactions(ctx)
	ticker := time.NewTicker(w.maintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runCompactions(ctx)
		}
	}
}

func (w *balanceHistoryMaintenanceWorker) runCompactions(ctx context.Context) {
	for range w.maxCompactionsPerPass {
		compacted, err := w.compact(ctx, w.compactionThreshold)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				w.logger.Errorf("Historical-balance compaction pass failed: %v", err)
			}

			return
		}
		if !compacted {
			return
		}
	}
}
