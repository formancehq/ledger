package dal

import (
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
)

const (
	// idleL0Threshold is the target L0 file count below which we don't compact.
	// Even a few L0 files with a cold cache cause read amplification, so we
	// keep this low.
	idleL0Threshold = 4

	// defaultIdleCheckInterval is how often we check for idle compaction.
	defaultIdleCheckInterval = 30 * time.Second
)

// IdleCompactor monitors Pebble's L0 file count and triggers a full compaction
// when the database is idle (no new flushes) and L0 exceeds idleL0Threshold.
// This prevents L0 files from accumulating below the runtime compaction threshold
// (e.g. 64) and causing slow reads on restart with a cold block cache.
type IdleCompactor struct {
	store  *Store
	logger logging.Logger
	w      worker.Worker

	interval       time.Duration
	prevFlushCount atomic.Int64
	compacting     atomic.Bool
}

// NewIdleCompactor creates a new IdleCompactor for the given store.
func NewIdleCompactor(store *Store, logger logging.Logger) *IdleCompactor {
	return &IdleCompactor{
		store:    store,
		logger:   logger.WithField("cmp", "idle-compactor"),
		w:        worker.New(),
		interval: defaultIdleCheckInterval,
	}
}

// Start launches the background goroutine that periodically checks for idle compaction.
func (c *IdleCompactor) Start() {
	// Seed the previous flush count so the first tick doesn't immediately compact.
	db := c.store.getDB()
	c.prevFlushCount.Store(int64(db.Metrics().Flush.Count))

	c.w.Run(func(stop <-chan struct{}) {
		worker.RunTicker(stop, c.interval, c.check)
	})
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (c *IdleCompactor) Stop() {
	c.w.Stop()
}

func (c *IdleCompactor) check() {
	if c.compacting.Load() {
		return
	}

	db := c.store.getDB()
	m := db.Metrics()

	currentFlushCount := int64(m.Flush.Count)
	prevFlushCount := c.prevFlushCount.Swap(currentFlushCount)

	idle := currentFlushCount == prevFlushCount
	l0Files := m.Levels[0].NumFiles

	if !idle || l0Files <= idleL0Threshold {
		return
	}

	c.compacting.Store(true)
	c.logger.WithFields(map[string]any{
		"l0FileCount": l0Files,
		"l0Size":      m.Levels[0].Size,
	}).Infof("Idle detected with elevated L0, starting compaction")

	go func() {
		defer c.compacting.Store(false)
		start := time.Now()
		if err := db.Compact(nil, []byte{0xFF}, false); err != nil {
			c.logger.WithFields(map[string]any{
				"error": err,
			}).Infof("Idle compaction failed (non-fatal)")
			return
		}
		m2 := db.Metrics()
		c.logger.WithFields(map[string]any{
			"duration":    time.Since(start).String(),
			"l0FileCount": m2.Levels[0].NumFiles,
			"l0Size":      m2.Levels[0].Size,
		}).Infof("Idle compaction complete")
	}()
}
