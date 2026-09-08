package readstore

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// LedgerHistoryStateEntry is the storage codec result for one indexbuilder
// tracker row. Interpretation of State belongs to indexbuilder.
type LedgerHistoryStateEntry struct {
	LedgerName string
	State      byte
}

// ReadAllLedgerHistoryStatesFrom reads a coherent tracker snapshot. Malformed
// keys and values are corruption, never silently treated as an absent ledger.
func ReadAllLedgerHistoryStatesFrom(reader dal.PebbleReader) ([]LedgerHistoryStateEntry, error) {
	prefix := LedgerHistoryStatePrefix()
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: IncrementBytes(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("creating ledger history state iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var entries []LedgerHistoryStateEntry
	for iter.First(); iter.Valid(); iter.Next() {
		suffix := iter.Key()[len(prefix):]
		if len(suffix) != dal.LedgerNameFixedSize {
			return nil, fmt.Errorf("corrupt ledger history key: got %d-byte suffix, want %d", len(suffix), dal.LedgerNameFixedSize)
		}

		ledgerName, err := parseLedgerNameFixed(suffix)
		if err != nil {
			return nil, fmt.Errorf("corrupt ledger history key: %w", err)
		}
		if ledgerName == "" {
			return nil, errors.New("corrupt ledger history key: empty ledger name")
		}

		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading ledger history state for %q: %w", ledgerName, err)
		}
		if len(value) != 1 {
			return nil, fmt.Errorf("corrupt ledger history state for %q: got %d bytes, want 1", ledgerName, len(value))
		}

		entries = append(entries, LedgerHistoryStateEntry{LedgerName: ledgerName, State: value[0]})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating ledger history states: %w", err)
	}

	return entries, nil
}
