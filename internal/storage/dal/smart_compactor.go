package dal

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/signal"
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
}

// allCompactPrefixes returns all prefix ranges for a full compaction (cold + system + attributes).
var allCompactPrefixes = append(coldPrefixes,
	compactPrefix{"per-ledger-system", ZonePerLedgerSysStart, ZonePerLedgerSysEnd},
	compactPrefix{"attributes", ZoneAttributesStart, ZoneAttributesEnd},
	compactPrefix{"global-system", ZoneGlobalSysStart, ZoneGlobalSysEnd},
)

// sequencePrefixes are the cold prefixes keyed by [prefix][sequence_uint64_BE].
// These support incremental range compaction.
var sequencePrefixes = []byte{KeyPrefixLog, KeyPrefixAudit}

// SmartCompactor triggers prefix-aware compaction of the cold zone (logs,
// audit, tx-updates) to keep the LSM tree well-organized:
//   - Incrementally when 10,000 new logs have been written (small range compactions)
//   - On post-purge signals from the FSM (full prefix compact to push tombstones)
type SmartCompactor struct {
	store         *Store
	logger        logging.Logger
	notifications *signal.Notifications
	w             worker.Worker

	threshold     uint64
	compacting    atomic.Bool
	compactWg     sync.WaitGroup
	coldRequestCh <-chan struct{}

	// lastCompactedSeq tracks the last sequence for which an incremental
	// compaction was completed. Only accessed from the main loop goroutine.
	lastCompactedSeq uint64

	// stopCh is the worker's stop channel, stored by Start() so that background
	// compaction goroutines can check for shutdown between prefix iterations.
	stopCh <-chan struct{}
}

// NewSmartCompactor creates a new SmartCompactor for the given store.
// coldRequestCh receives signals from the FSM when a period purge has been applied,
// indicating that cold zone compaction would be beneficial.
// notifications provides the LogCommitted signal and LastSequence atomic.
func NewSmartCompactor(
	store *Store,
	logger logging.Logger,
	notifications *signal.Notifications,
	coldRequestCh <-chan struct{},
	threshold uint64,
) *SmartCompactor {
	return &SmartCompactor{
		store:         store,
		logger:        logger.WithField("cmp", "smart-compactor"),
		notifications: notifications,
		w:             worker.New(),
		coldRequestCh: coldRequestCh,
		threshold:     threshold,
	}
}

// Start launches the background goroutine that listens for log commits and
// post-purge signals to trigger compaction.
func (c *SmartCompactor) Start() {
	c.w.Run(func(stop <-chan struct{}) {
		c.stopCh = stop

		// Seed lastCompactedSeq so we don't try to compact the entire
		// history on first start.
		if seq := c.notifications.LastSequence.Load(); seq > 0 {
			c.lastCompactedSeq = seq
		}

		for {
			select {
			case <-stop:
				return
			case <-c.coldRequestCh:
				c.compactPrefixes("post-purge", coldPrefixes)
			case <-c.notifications.LogCommitted.C():
				c.maybeIncrementalCompact()
			}
		}
	})
}

// maybeIncrementalCompact checks if enough new logs have been written since
// the last compaction and triggers a range-scoped compaction of just the new
// entries. This keeps each compaction small and bounded in memory.
func (c *SmartCompactor) maybeIncrementalCompact() {
	currentSeq := c.notifications.LastSequence.Load()
	if currentSeq <= c.lastCompactedSeq {
		return
	}

	delta := currentSeq - c.lastCompactedSeq
	if delta < c.threshold {
		return
	}

	fromSeq := c.lastCompactedSeq + 1
	toSeq := currentSeq

	if c.compactSequenceRange("incremental", fromSeq, toSeq) {
		c.lastCompactedSeq = toSeq
	}
}

// compactSequenceRange compacts the key range [prefix|fromSeq, prefix|toSeq+1)
// for each sequence-keyed cold prefix (logs, audit). This is much lighter than
// compacting the entire prefix since it only touches the new SSTables.
// Returns true if the compaction was accepted (false if another is in progress).
func (c *SmartCompactor) compactSequenceRange(reason string, fromSeq, toSeq uint64) bool {
	if !c.compacting.CompareAndSwap(false, true) {
		return false
	}

	c.compactWg.Go(func() {
		defer c.compacting.Store(false)

		start := time.Now()
		db := c.store.getDB()

		for _, prefix := range sequencePrefixes {
			select {
			case <-c.stopCh:
				return
			default:
			}

			startKey := sequenceKey(prefix, fromSeq)
			endKey := sequenceKey(prefix, toSeq+1)

			err := db.Compact(startKey, endKey, false)
			if err != nil {
				c.logger.WithFields(map[string]any{
					"reason":  reason,
					"prefix":  fmt.Sprintf("0x%02x", prefix),
					"fromSeq": fromSeq,
					"toSeq":   toSeq,
					"error":   err,
				}).Infof("Incremental compaction failed (non-fatal)")

				continue
			}
		}

		c.logger.WithFields(map[string]any{
			"reason":   reason,
			"fromSeq":  fromSeq,
			"toSeq":    toSeq,
			"count":    toSeq - fromSeq + 1,
			"duration": time.Since(start).String(),
		}).Infof("Incremental compaction complete")
	})

	return true
}

// sequenceKey builds a Pebble key: [prefix][sequence_uint64_BE].
func sequenceKey(prefix byte, seq uint64) []byte {
	key := make([]byte, 9)
	key[0] = prefix
	binary.BigEndian.PutUint64(key[1:], seq)

	return key
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
		err := db.Compact([]byte{p.start}, []byte{p.end}, false)
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
		"l0FileCount": m.Levels[0].NumFiles,
		"l0Size":      m.Levels[0].Size,
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

			err := db.Compact([]byte{p.start}, []byte{p.end}, false)
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
			"l0FileCount": m2.Levels[0].NumFiles,
			"l0Size":      m2.Levels[0].Size,
		}).Infof("All prefix compactions complete")
	})
}
