package readstore

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ledgerScopedPrefixes lists read-index and local build-state prefixes that the
// live DeleteLedger fold must wipe (keyed by
// [prefix...][ledgerName padded 64B]...).
//
// Every index/build entry MUST be wiped by the live DeleteLedger path — missing one leaks
// rows past a ledger drop, and a same-name recreate then resurrects
// stale state under the new ledger. The per-replica IndexVersionState
// is the load-bearing example: a `CurrentVersion=2` row outliving the
// ledger that wrote it would make a freshly recreated index's queries
// scan an empty v=2 keyspace instead of returning ErrIndexBuilding,
// and `purgeOrphanVersions` at boot would GC v=1 (and others) against
// a config that no longer exists.
var ledgerScopedPrefixes = [][]byte{
	{PrefixMetadataIndex},
	{PrefixEntityExists},
	{PrefixReverseMap},
	{PrefixAccountTx},
	{PrefixSourceAccountTx},
	{PrefixDestinationAccountTx},
	{PrefixTransactionReference},
	{PrefixTransactionTimestamp},
	{PrefixLedgerLogs},
	{PrefixLedgerLogDate},
	{PrefixTransactionInsertedAt},
	{PrefixTransactionRevertedAt},
	{PrefixAccountByAsset},
	{PrefixInternal, SubInternalBackfill},
	{PrefixInternal, SubInternalIndexVersion},
}

// SubInternalLedgerHistory is deliberately absent. Historical index backfills
// replay old DeleteLedger entries while building a later same-name incarnation;
// erasing the current incarnation's history tracker there would corrupt the
// CreateIndex decision. The live processLogs path deletes that tracker
// explicitly through its counted WriteBatch.

// DeleteLedgerIndexes removes all read index data for the given ledger.
// It performs range deletes on all ledger-scoped prefixes:
// [prefix...][ledgerName padded 64B] -> successor of that padded block.
//
// Validation guarantees ledger names are printable ASCII only, so the last
// padding byte is never 0xFF — incrementing it cannot carry past the
// fixed-width name block and yields a clean exclusive upper bound.
func DeleteLedgerIndexes(batch *dal.WriteSession, ledgerName string) error {
	for _, prefix := range ledgerScopedPrefixes {
		start := make([]byte, 0, len(prefix)+dal.LedgerNameFixedSize)
		start = append(start, prefix...)
		start = appendPaddedLedgerName(start, ledgerName)

		end := make([]byte, 0, len(prefix)+dal.LedgerNameFixedSize)
		end = append(end, prefix...)
		end = appendPaddedLedgerName(end, ledgerName)
		end[len(end)-1]++

		if err := batch.DeleteRangeNoSync(start, end); err != nil {
			return fmt.Errorf("deleting readstore prefix %x for ledger %q: %w", prefix, ledgerName, err)
		}
	}

	return nil
}

// DeleteLedgerIndexPrefix removes one concrete index keyspace for a ledger.
// Backfill replay uses this narrower primitive when it crosses a historical
// DeleteLedger: the task must discard rows from the old generation without
// deleting current-generation IndexVersionState, cursors, or history tracker.
func DeleteLedgerIndexPrefix(batch *dal.WriteSession, prefix byte, ledgerName string) error {
	start := make([]byte, 0, 1+dal.LedgerNameFixedSize)
	start = append(start, prefix)
	start = appendPaddedLedgerName(start, ledgerName)

	end := append([]byte(nil), start...)
	end[len(end)-1]++

	if err := batch.DeleteRangeNoSync(start, end); err != nil {
		return fmt.Errorf("deleting readstore index prefix %x for ledger %q: %w", prefix, ledgerName, err)
	}

	return nil
}

// appendPaddedLedgerName appends the ledger name zero-padded to
// dal.LedgerNameFixedSize bytes. Callers MUST validate the name length
// upstream — copy() truncates silently here.
func appendPaddedLedgerName(dst []byte, name string) []byte {
	var pad [dal.LedgerNameFixedSize]byte
	copy(pad[:], name)

	return append(dst, pad[:]...)
}
