package attributes

import (
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// CreateBaselineSnapshot copies the entire attribute zone (every attribute
// type, final value per canonical key) from the source reader into a compact
// Pebble DB at destPath. No history, no logs. It also copies the LedgerInfo
// entries from the Global zone (query.ReadLedgers reads them from there) so the
// checker can verify the schema / account-type / presence projections against
// this boundary-time baseline rather than the live store.
//
// The whole attribute zone is copied (not just the types a compare pass reads
// today) so every checker baseline read — compareVolumes / compareMetadata /
// compareTransactions / compareReferences / compareBoundaries /
// compareMirrorV2LogID (its archived floor lives on the Boundary rows) and the
// skip-order folds (foldBaselineReferences / foldBaselineBoundaries /
// foldBaselineLedgers) — resolves against real pre-archive state instead of a
// genesis/empty fallback, and a future compare pass over any attribute needs no
// change here.
//
// The result is orders of magnitude smaller than a full Pebble checkpoint
// because it contains only the attributes zone plus LedgerInfo, not the entire
// store. This is critical for archived chapters: the whole point of archiving
// is to reclaim disk space, so a full checkpoint would be counter-productive.
//
// The write uses atomic rename: data is written to a temporary directory
// first, then renamed to destPath. This eliminates TOCTOU races with
// concurrent readers (the checker).
func CreateBaselineSnapshot(reader dal.PebbleReader, destPath string) error {
	// Write to a temporary sibling directory, then atomic rename.
	tmpPath := destPath + fmt.Sprintf(".tmp-%d-%d", os.Getpid(), time.Now().UnixNano())

	if err := os.MkdirAll(tmpPath, 0755); err != nil {
		return fmt.Errorf("creating temp baseline dir: %w", err)
	}

	// Clean up temp on failure
	success := false

	defer func() {
		if !success {
			_ = os.RemoveAll(tmpPath)
		}
	}()

	db, err := pebble.Open(tmpPath, &pebble.Options{
		Logger:     dal.DiscardPebbleLogger(),
		DisableWAL: true,
	})
	if err != nil {
		return fmt.Errorf("opening temp baseline db: %w", err)
	}

	// Write all computed attribute values into the baseline DB.
	if err := writeBaselineAttributes(reader, db); err != nil {
		_ = db.Close()

		return err
	}

	if err := db.Flush(); err != nil {
		_ = db.Close()

		return fmt.Errorf("flushing baseline db: %w", err)
	}

	if err := db.Close(); err != nil {
		return fmt.Errorf("closing baseline db: %w", err)
	}

	// Atomic swap: remove old, rename temp → dest.
	_ = os.RemoveAll(destPath)

	if err := os.Rename(tmpPath, destPath); err != nil {
		return fmt.Errorf("renaming baseline snapshot: %w", err)
	}

	success = true

	return nil
}

// writeBaselineAttributes copies the whole attribute zone verbatim from the
// source reader into the baseline DB, then the Global-zone LedgerInfo entries.
func writeBaselineAttributes(reader dal.PebbleReader, db *pebble.DB) error {
	if err := copyBaselineRange(reader, db, []byte{dal.ZoneAttributes}, []byte{dal.ZoneAttributes + 1}); err != nil {
		return fmt.Errorf("writing baseline attributes: %w", err)
	}

	if err := copyBaselineLedgers(reader, db); err != nil {
		return fmt.Errorf("writing baseline ledgers: %w", err)
	}

	return nil
}

// copyBaselineRange copies every [lower, upper) key/value verbatim from the
// source reader into the baseline DB, preserving the exact key layout.
func copyBaselineRange(reader dal.PebbleReader, db *pebble.DB, lower, upper []byte) error {
	iter, err := dal.NewBoundedIter(reader, lower, upper)
	if err != nil {
		return fmt.Errorf("iterating range [% x, % x): %w", lower, upper, err)
	}

	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		// Pebble copies key and value on Set, so the iterator's transient slices
		// are safe to pass.
		if err := db.Set(iter.Key(), iter.Value(), pebble.NoSync); err != nil {
			return fmt.Errorf("writing baseline entry: %w", err)
		}
	}

	return iter.Error()
}

// copyBaselineLedgers copies the LedgerInfo entries (zone ZoneGlobal /
// SubGlobLedgerInfo) verbatim into the baseline DB, preserving the exact key
// layout so query.ReadLedgers reads them back identically. LedgerInfo also
// lives in the attribute zone (SubAttrLedger) which the range copy above
// already carries; this Global copy is what the checker's ReadLedgers path
// reads.
func copyBaselineLedgers(reader dal.PebbleReader, db *pebble.DB) error {
	return copyBaselineRange(reader, db,
		[]byte{dal.ZoneGlobal, dal.SubGlobLedgerInfo},
		[]byte{dal.ZoneGlobal, dal.SubGlobLedgerInfo + 1})
}
