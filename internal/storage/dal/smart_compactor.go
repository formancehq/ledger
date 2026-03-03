package dal

import (
	"fmt"
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

	// defaultIdleCheckInterval is how often we check for compaction.
	defaultIdleCheckInterval = 30 * time.Second
)

// compactPrefix defines a key-range [start, end) that db.Compact covers.
type compactPrefix struct {
	name  string
	start byte
	end   byte
}

// coldPrefixes are write-once, immutable after creation. Safe to compact even
// under active traffic — no concurrent writes target these ranges.
var coldPrefixes = []compactPrefix{
	{"logs", KeyPrefixLog, KeyPrefixLog + 1},
	{"audit", KeyPrefixAudit, KeyPrefixAudit + 1},
	{"tx-updates", KeyPrefixTransactionUpdate, KeyPrefixTransactionUpdate + 1},
}

// attributesPrefix is actively written (generation-rotation pruning).
// Only compact when the node is idle to avoid interfering with writes.
var attributesPrefix = compactPrefix{"attributes", KeyPrefixAttributes, KeyPrefixAttributes + 1}

// allPrefixes returns cold + attributes prefixes for full compaction (e.g. startup).
func allPrefixes() []compactPrefix {
	return append(coldPrefixes, attributesPrefix)
}

// SmartCompactor monitors Pebble's L0 file count and triggers prefix-aware compaction:
//
//   - Periodic compaction: when L0 exceeds idleL0Threshold, compact cold prefixes
//     (logs, audit, tx-updates) one at a time. These are write-once and safe to
//     compact under traffic. If additionally idle (no new flushes), also compact
//     the attributes prefix.
//   - Cold compaction: triggered on-demand (via coldRequestCh) after period archival
//     purges data in the cold zone. Compacts only cold prefixes.
//
// Each db.Compact covers a single prefix, so Pebble only buffers one prefix's
// worth of data at a time — reducing peak memory versus a full-range compaction.
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

// Start launches the background goroutine that listens for periodic ticks and cold compaction requests.
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
				c.periodicCheck()
			case <-c.coldRequestCh:
				c.compactPrefixes("post-purge", coldPrefixes)
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

// periodicCheck runs every interval. If L0 is above threshold, it compacts cold
// prefixes unconditionally (write-once data, safe under traffic). If additionally
// idle (no new flushes since last check), it also compacts attributes.
func (c *SmartCompactor) periodicCheck() {
	if c.compacting.Load() {
		return
	}

	db := c.store.getDB()
	m := db.Metrics()

	currentFlushCount := int64(m.Flush.Count)
	prevFlushCount := c.prevFlushCount.Swap(currentFlushCount)
	idle := currentFlushCount == prevFlushCount

	l0Files := m.Levels[0].NumFiles
	if l0Files <= idleL0Threshold {
		return
	}

	if idle {
		c.compactPrefixes("idle", allPrefixes())
	} else {
		c.compactPrefixes("periodic", coldPrefixes)
	}
}

// CompactAll runs a synchronous prefix-by-prefix compaction of the entire
// Pebble keyspace. It reuses the same prefix list as the background
// SmartCompactor but blocks until all prefixes are compacted.
// Returns the first error encountered.
func (s *Store) CompactAll() error {
	db := s.getDB()
	for _, p := range allPrefixes() {
		if err := db.Compact([]byte{p.start}, []byte{p.end}, false); err != nil {
			return fmt.Errorf("compacting prefix %s: %w", p.name, err)
		}
	}
	return nil
}

// compactPrefixes runs db.Compact for each prefix sequentially in a background
// goroutine. Guarded by the compacting atomic to prevent concurrent compactions.
func (c *SmartCompactor) compactPrefixes(reason string, prefixes []compactPrefix) {
	if !c.compacting.CompareAndSwap(false, true) {
		c.logger.WithFields(map[string]any{
			"reason": reason,
		}).Infof("Skipping compaction (another compaction in progress)")
		return
	}

	db := c.store.getDB()
	m := db.Metrics()

	c.logger.WithFields(map[string]any{
		"reason":      reason,
		"prefixCount": len(prefixes),
		"l0FileCount": m.Levels[0].NumFiles,
		"l0Size":      m.Levels[0].Size,
	}).Infof("Starting prefix-by-prefix compaction")

	c.compactWg.Add(1)
	go func() {
		defer c.compactWg.Done()
		defer c.compacting.Store(false)

		overallStart := time.Now()
		for _, p := range prefixes {
			prefixStart := time.Now()
			if err := db.Compact([]byte{p.start}, []byte{p.end}, false); err != nil {
				c.logger.WithFields(map[string]any{
					"reason": reason,
					"prefix": p.name,
					"error":  err,
				}).Infof("Prefix compaction failed (non-fatal)")
				continue
			}
			c.logger.WithFields(map[string]any{
				"reason":   reason,
				"prefix":   p.name,
				"duration": time.Since(prefixStart).String(),
			}).Infof("Prefix compaction complete")
		}

		m2 := db.Metrics()
		c.logger.WithFields(map[string]any{
			"reason":      reason,
			"duration":    time.Since(overallStart).String(),
			"l0FileCount": m2.Levels[0].NumFiles,
			"l0Size":      m2.Levels[0].Size,
		}).Infof("All prefix compactions complete")
	}()
}
