package dal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/worker"
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
// coldRequestCh receives signals from the FSM when a chapter purge has been applied,
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

// compactRange compacts the single key range [start, end) under dbMu.RLock,
// returning ErrStoreClosed if the DB has been closed or swapped by a restore.
// Holding the lock for one Compact call bounds a concurrent Close/RestoreCheckpoint
// wait to a single prefix compaction, matching SmartCompactor.Stop's documented bound.
func (s *Store) compactRange(start, end byte) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return ErrStoreClosed
	}

	return db.Compact(context.Background(), []byte{start}, []byte{end}, false)
}

// metricsIfOpen returns Pebble metrics under dbMu.RLock, or (nil, false) if the
// DB has been closed. Used for best-effort compaction logging.
func (s *Store) metricsIfOpen() (*pebble.Metrics, bool) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return nil, false
	}

	return db.Metrics(), true
}

// CompactAll runs a synchronous prefix-by-prefix compaction of the entire
// Pebble keyspace. It reuses the same prefix list as the background
// SmartCompactor but blocks until all prefixes are compacted.
// Returns the first error encountered (including ErrStoreClosed if the store closes).
func (s *Store) CompactAll() error {
	for _, p := range allCompactPrefixes {
		if err := s.compactRange(p.start, p.end); err != nil {
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

	m, ok := c.store.metricsIfOpen()
	if !ok {
		c.compacting.Store(false)
		c.logger.WithFields(map[string]any{
			"reason": reason,
		}).Infof("Skipping compaction (store closed)")

		return
	}

	c.logger.WithFields(map[string]any{
		"reason":      reason,
		"prefixCount": len(prefixes),
		"l0FileCount": m.Levels[0].TablesCount,
		"l0Size":      m.Levels[0].TablesSize,
	}).Infof("Starting prefix-by-prefix compaction")

	c.compactWg.Go(func() {
		c.runCompaction(reason, prefixes)
	})
}

// runCompaction compacts each prefix sequentially through the guarded
// compactRange helper. It is the body of the background compaction goroutine,
// extracted so it can be exercised synchronously in tests. The caller must have
// set the compacting flag; runCompaction resets it on return.
func (c *SmartCompactor) runCompaction(reason string, prefixes []compactPrefix) {
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

		if err := c.store.compactRange(p.start, p.end); err != nil {
			if errors.Is(err, ErrStoreClosed) {
				c.logger.WithFields(map[string]any{
					"reason": reason,
				}).Infof("Compaction aborted (store closed)")

				return
			}

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

	if m2, ok := c.store.metricsIfOpen(); ok {
		c.logger.WithFields(map[string]any{
			"reason":      reason,
			"duration":    time.Since(overallStart).String(),
			"l0FileCount": m2.Levels[0].TablesCount,
			"l0Size":      m2.Levels[0].TablesSize,
		}).Infof("All prefix compactions complete")
	}
}
