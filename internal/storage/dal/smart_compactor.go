package dal

import (
	"sync"
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

// SmartCompactor monitors Pebble's L0 file count and triggers zone-aware compaction:
//
//   - Idle compaction: when no new flushes occur and L0 exceeds idleL0Threshold,
//     compact only the attributes zone [0xF1, 0xF2) where DeleteRange tombstones
//     from generation-rotation pruning benefit from compaction.
//   - Cold compaction: triggered on-demand (via coldRequestCh) after period archival
//     purges data in the cold zone [0x01, 0xF1). This pushes tombstones down the LSM
//     to reclaim space.
//
// The cold zone (logs, audit, tx updates) is immutable write-once data that does not
// benefit from compaction unless a purge just deleted data. The system zone is tiny
// and handled natively by Pebble. Startup compaction remains full-range (in store.go).
type SmartCompactor struct {
	store  *Store
	logger logging.Logger
	w      worker.Worker

	interval       time.Duration
	prevFlushCount atomic.Int64
	compacting     atomic.Bool
	compactWg      sync.WaitGroup
	coldRequestCh  <-chan struct{}
}

// NewSmartCompactor creates a new SmartCompactor for the given store.
// coldRequestCh receives signals from the FSM when a period purge has been applied,
// indicating that cold zone compaction would be beneficial.
func NewSmartCompactor(store *Store, logger logging.Logger, coldRequestCh <-chan struct{}) *SmartCompactor {
	return &SmartCompactor{
		store:         store,
		logger:        logger.WithField("cmp", "smart-compactor"),
		w:             worker.New(),
		interval:      defaultIdleCheckInterval,
		coldRequestCh: coldRequestCh,
	}
}

// Start launches the background goroutine that listens for idle ticks and cold compaction requests.
func (c *SmartCompactor) Start() {
	db := c.store.getDB()
	c.prevFlushCount.Store(int64(db.Metrics().Flush.Count))

	c.w.Run(func(stop <-chan struct{}) {
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				c.checkIdle()
			case <-c.coldRequestCh:
				c.compactZone("cold", []byte{KeyPrefixLog}, []byte{KeyPrefixAttributes})
			}
		}
	})
}

// Stop signals the background goroutine to stop, then waits for any in-flight
// compaction goroutine to finish before returning.
func (c *SmartCompactor) Stop() {
	c.w.Stop()
	c.compactWg.Wait()
}

// checkIdle detects idle periods (no new flushes) and triggers attributes-zone compaction.
func (c *SmartCompactor) checkIdle() {
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

	c.compactZone("attributes", []byte{KeyPrefixAttributes}, []byte{KeyPrefixAttributes + 1})
}

// compactZone runs db.Compact(start, end) in a background goroutine with logging.
// Guarded by the compacting atomic to prevent concurrent compactions.
func (c *SmartCompactor) compactZone(zone string, start, end []byte) {
	if !c.compacting.CompareAndSwap(false, true) {
		c.logger.WithFields(map[string]any{
			"zone": zone,
		}).Infof("Skipping zone compaction (another compaction in progress)")
		return
	}

	db := c.store.getDB()
	m := db.Metrics()
	l0Files := m.Levels[0].NumFiles

	c.logger.WithFields(map[string]any{
		"zone":        zone,
		"l0FileCount": l0Files,
		"l0Size":      m.Levels[0].Size,
	}).Infof("Starting zone compaction")

	c.compactWg.Add(1)
	go func() {
		defer c.compactWg.Done()
		defer c.compacting.Store(false)
		compactStart := time.Now()
		if err := db.Compact(start, end, false); err != nil {
			c.logger.WithFields(map[string]any{
				"zone":  zone,
				"error": err,
			}).Infof("Zone compaction failed (non-fatal)")
			return
		}
		m2 := db.Metrics()
		c.logger.WithFields(map[string]any{
			"zone":        zone,
			"duration":    time.Since(compactStart).String(),
			"l0FileCount": m2.Levels[0].NumFiles,
			"l0Size":      m2.Levels[0].Size,
		}).Infof("Zone compaction complete")
	}()
}
