package dal

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
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

// allCompactPrefixes returns all prefix ranges for a full compaction (cold + attributes).
var allCompactPrefixes = append(coldPrefixes, compactPrefix{
	"attributes", KeyPrefixAttributes, KeyPrefixAttributes + 1,
})

// SmartCompactor listens for post-purge signals and triggers prefix-aware
// compaction of the cold zone to push tombstones down the LSM and reclaim space.
//
// Periodic and startup compaction are no longer needed: the low
// L0CompactionThreshold (4) ensures Pebble keeps L0 clean natively, and
// the extended block cache warmup covers [0xF1, 0xFF) on startup.
type SmartCompactor struct {
	store  *Store
	logger logging.Logger
	w      worker.Worker

	compacting    atomic.Bool
	compactWg     sync.WaitGroup
	coldRequestCh <-chan struct{}
}

// NewSmartCompactor creates a new SmartCompactor for the given store.
// coldRequestCh receives signals from the FSM when a period purge has been applied,
// indicating that cold zone compaction would be beneficial.
func NewSmartCompactor(store *Store, logger logging.Logger, coldRequestCh <-chan struct{}) *SmartCompactor {
	return &SmartCompactor{
		store:         store,
		logger:        logger.WithField("cmp", "smart-compactor"),
		w:             worker.New(),
		coldRequestCh: coldRequestCh,
	}
}

// Start launches the background goroutine that listens for cold compaction requests.
func (c *SmartCompactor) Start() {
	c.w.Run(func(stop <-chan struct{}) {
		for {
			select {
			case <-stop:
				return
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

// CompactAll runs a synchronous prefix-by-prefix compaction of the entire
// Pebble keyspace. It reuses the same prefix list as the background
// SmartCompactor but blocks until all prefixes are compacted.
// Returns the first error encountered.
func (s *Store) CompactAll() error {
	db := s.getDB()
	for _, p := range allCompactPrefixes {
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
