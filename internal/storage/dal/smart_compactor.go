package dal

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

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
	{"logs", ZoneCold, ZoneCold + 1},
}

// allCompactPrefixes returns all prefix ranges for a full compaction (all zones).
var allCompactPrefixes = append(coldPrefixes,
	compactPrefix{"attributes", ZoneAttributes, ZoneAttributes + 1},
	compactPrefix{"cache", ZoneCache, ZoneCache + 1},
	compactPrefix{"per-ledger", ZonePerLedger, ZonePerLedger + 1},
	compactPrefix{"idempotency", ZoneIdempotency, ZoneIdempotency + 1},
	compactPrefix{"global", ZoneGlobal, ZoneGlobal + 1},
)

// SmartCompactor triggers prefix-aware compaction of the cold zone (logs,
// audit) on post-purge signals from the FSM to push tombstones down through
// the LSM tree and reclaim disk space promptly.
type SmartCompactor struct {
	store         *Store
	logger        logging.Logger
	w             worker.Worker
	compacting    atomic.Bool
	compactWg     sync.WaitGroup
	coldRequestCh <-chan struct{}

	// stopCh is the worker's stop channel, stored by Start() so that background
	// compaction goroutines can check for shutdown between prefix iterations.
	stopCh <-chan struct{}
}

// NewSmartCompactor creates a new SmartCompactor for the given store.
// coldRequestCh receives signals from the FSM when a period purge has been applied,
// indicating that cold zone compaction would be beneficial.
func NewSmartCompactor(
	store *Store,
	logger logging.Logger,
	coldRequestCh <-chan struct{},
) *SmartCompactor {
	return &SmartCompactor{
		store:         store,
		logger:        logger.WithField("cmp", "smart-compactor"),
		w:             worker.New(),
		coldRequestCh: coldRequestCh,
	}
}

// Start launches the background goroutine that listens for post-purge signals
// to trigger compaction.
func (c *SmartCompactor) Start() {
	c.w.Run(func(stop <-chan struct{}) {
		c.stopCh = stop

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
// compaction goroutine to finish before returning. Compaction goroutines check
// stopCh between prefix iterations, so they abort quickly; the maximum wait is
// a single db.Compact call on one prefix range.
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
		err := db.Compact(context.Background(), []byte{p.start}, []byte{p.end}, false)
		if err != nil {
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
		"l0FileCount": m.Levels[0].TablesCount,
		"l0Size":      m.Levels[0].TablesSize,
	}).Infof("Starting prefix-by-prefix compaction")

	c.compactWg.Go(func() {
		defer c.compacting.Store(false)

		overallStart := time.Now()

		for _, p := range prefixes {
			// Check for shutdown between prefix iterations so we abort early
			// rather than starting another potentially long db.Compact call.
			select {
			case <-c.stopCh:
				c.logger.WithFields(map[string]any{
					"reason": reason,
				}).Infof("Compaction aborted due to shutdown")

				return
			default:
			}

			prefixStart := time.Now()

			err := db.Compact(context.Background(), []byte{p.start}, []byte{p.end}, false)
			if err != nil {
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
			"l0FileCount": m2.Levels[0].TablesCount,
			"l0Size":      m2.Levels[0].TablesSize,
		}).Infof("All prefix compactions complete")
	})
}
