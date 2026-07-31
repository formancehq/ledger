package bootstrap

import (
	"context"
	"errors"
	"sync"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

type balanceHistoryTierFunc func(context.Context) (int, error)

type balanceHistoryCompactFunc func(context.Context, int) (bool, error)

type balanceHistoryCollectFunc func(
	context.Context,
	balancehistorystore.RemoteGCBudget,
) (balancehistorystore.RemoteGCResult, error)

// balanceHistoryMaintenanceWorker owns all physical PIT-history maintenance.
// The builder only publishes local immutable runs; compaction, tiering, and
// reclamation run here so cold hydration or remote I/O never stalls the
// authoritative-source watermark.
type balanceHistoryMaintenanceWorker struct {
	logger                logging.Logger
	maintenanceInterval   time.Duration
	compactionThreshold   int
	maxCompactionsPerPass int
	tierInterval          time.Duration
	remoteGCInterval      time.Duration
	remoteGCBudget        balancehistorystore.RemoteGCBudget
	compact               balanceHistoryCompactFunc
	tier                  balanceHistoryTierFunc
	collect               balanceHistoryCollectFunc

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func newBalanceHistoryMaintenanceWorker(
	logger logging.Logger,
	config BalanceHistoryConfig,
	compact balanceHistoryCompactFunc,
	tier balanceHistoryTierFunc,
	collect balanceHistoryCollectFunc,
) *balanceHistoryMaintenanceWorker {
	return &balanceHistoryMaintenanceWorker{
		logger:                logger,
		maintenanceInterval:   config.MaintenanceInterval,
		compactionThreshold:   config.RunCompactionThreshold,
		maxCompactionsPerPass: config.MaxCompactionsPerPass,
		tierInterval:          config.TierInterval,
		remoteGCInterval:      config.RemoteGCInterval,
		remoteGCBudget: balancehistorystore.RemoteGCBudget{
			ScanLimit:   config.RemoteGCScanLimit,
			DeleteLimit: config.RemoteGCDeleteLimit,
		},
		compact: compact,
		tier:    tier,
		collect: collect,
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
	w.cancel = cancel
	w.done = done
	w.mu.Unlock()

	go w.run(ctx, done)
}

func (w *balanceHistoryMaintenanceWorker) Stop() {
	if w == nil {
		return
	}

	w.mu.Lock()
	cancel := w.cancel
	done := w.done
	if cancel == nil {
		w.mu.Unlock()

		return
	}
	cancel()
	w.mu.Unlock()

	<-done

	w.mu.Lock()
	if w.done == done {
		w.cancel = nil
		w.done = nil
	}
	w.mu.Unlock()
}

func (w *balanceHistoryMaintenanceWorker) run(ctx context.Context, done chan<- struct{}) {
	defer close(done)

	// Start asynchronously with one bounded pass so restart recovery does not
	// wait a full production interval. Remote work remains conditional and runs
	// only after local maintenance succeeds.
	if w.runCompactions(ctx) && w.tier != nil && w.runTier(ctx) {
		w.runRemoteGC(ctx)
	}
	if ctx.Err() != nil {
		return
	}

	maintenanceTicker := time.NewTicker(w.maintenanceInterval)
	defer maintenanceTicker.Stop()

	var (
		tierTicker     *time.Ticker
		tierTick       <-chan time.Time
		remoteGCTicker *time.Ticker
		remoteGCTick   <-chan time.Time
	)
	if w.tier != nil {
		tierTicker = time.NewTicker(w.tierInterval)
		tierTick = tierTicker.C
		defer tierTicker.Stop()
	}
	if w.collect != nil {
		remoteGCTicker = time.NewTicker(w.remoteGCInterval)
		remoteGCTick = remoteGCTicker.C
		defer remoteGCTicker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-maintenanceTicker.C:
			w.runCompactions(ctx)
		case <-tierTick:
			w.runTier(ctx)
		case <-remoteGCTick:
			if w.runTier(ctx) {
				w.runRemoteGC(ctx)
			}
		}
	}
}

func (w *balanceHistoryMaintenanceWorker) runCompactions(ctx context.Context) bool {
	for range w.maxCompactionsPerPass {
		compacted, err := w.compact(ctx, w.compactionThreshold)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				w.logger.Errorf("Balance history compaction pass failed: %v", err)
			}

			return false
		}
		if !compacted {
			return true
		}
	}

	return true
}

func (w *balanceHistoryMaintenanceWorker) runTier(ctx context.Context) bool {
	if w.tier == nil {
		return true
	}
	_, err := w.tier(ctx)
	if err == nil {
		return true
	}
	if !errors.Is(err, context.Canceled) {
		w.logger.Errorf("Balance history cold-tier pass failed: %v", err)
	}

	return false
}

func (w *balanceHistoryMaintenanceWorker) runRemoteGC(ctx context.Context) {
	if w.collect == nil {
		return
	}
	_, err := w.collect(ctx, w.remoteGCBudget)
	if err != nil && !errors.Is(err, context.Canceled) {
		w.logger.Errorf("Balance history remote-GC pass failed: %v", err)
	}
}
