package usagestore

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ledgerScopedPrefixes lists all usage store key prefixes that contain
// ledger-scoped data (keyed by [prefix][ledgerName padded 64B]...). Every
// entry MUST be wiped by DeleteLedger — missing one leaks rows past a
// ledger drop.
var ledgerScopedPrefixes = [][]byte{
	{PrefixTemplate},
	{PrefixCounter},
}

// DeleteLedger removes all usage data for the given ledger. Performs range
// deletes on every ledger-scoped prefix: [prefix][ledgerName padded 64B] ->
// successor of that padded block.
//
// Validation guarantees ledger names are printable ASCII only, so the last
// padding byte is never 0xFF — incrementing it cannot carry past the
// fixed-width name block and yields a clean exclusive upper bound.
func DeleteLedger(batch *dal.WriteSession, ledgerName string) error {
	for _, prefix := range ledgerScopedPrefixes {
		start := make([]byte, 0, len(prefix)+dal.LedgerNameFixedSize)
		start = append(start, prefix...)
		start = appendPaddedLedgerName(start, ledgerName)

		end := make([]byte, 0, len(prefix)+dal.LedgerNameFixedSize)
		end = append(end, prefix...)
		end = appendPaddedLedgerName(end, ledgerName)
		end[len(end)-1]++

		if err := batch.DeleteRangeNoSync(start, end); err != nil {
			return fmt.Errorf("deleting usage store prefix %x for ledger %q: %w", prefix, ledgerName, err)
		}
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
