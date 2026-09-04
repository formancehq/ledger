package dal

import (
	"context"
	"fmt"
)

// compactPrefix defines a key-range [start, end) that db.Compact covers.
type compactPrefix struct {
	name  string
	start byte
	end   byte
}

// allCompactPrefixes returns all prefix ranges for a full compaction (all zones).
var allCompactPrefixes = []compactPrefix{
	{"history", ZoneHistory, ZoneHistory + 1},
	{"attributes", ZoneAttributes, ZoneAttributes + 1},
	{"cache", ZoneCache, ZoneCache + 1},
	{"per-ledger", ZonePerLedger, ZonePerLedger + 1},
	{"idempotency", ZoneIdempotency, ZoneIdempotency + 1},
	{"global", ZoneGlobal, ZoneGlobal + 1},
}

// compactRange compacts the single key range [start, end) under dbMu.RLock,
// returning ErrStoreClosed if the DB has been closed or swapped by a restore.
// Holding the lock for one Compact call bounds a concurrent Close/RestoreCheckpoint
// wait to a single prefix compaction.
func (s *Store) compactRange(start, end byte) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return ErrStoreClosed
	}

	return db.Compact(context.Background(), []byte{start}, []byte{end}, false)
}

// CompactAll runs a synchronous prefix-by-prefix compaction of the entire
// Pebble keyspace, blocking until all prefixes are compacted.
// Returns the first error encountered (including ErrStoreClosed if the store closes).
func (s *Store) CompactAll() error {
	for _, p := range allCompactPrefixes {
		if err := s.compactRange(p.start, p.end); err != nil {
			return fmt.Errorf("compacting prefix %s: %w", p.name, err)
		}
	}

	return nil
}
