package query

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadPendingLedgerCleanups reads all pending ledger cleanup entries from Pebble.
// Returns a map of ledger name -> delete log sequence number.
func ReadPendingLedgerCleanups(reader dal.PebbleReader) (map[string]uint64, error) {
	iter, err := dal.NewBoundedIter(reader, []byte{dal.ZonePerLedger, dal.SubPLPendingCleanup}, []byte{dal.ZonePerLedger, dal.SubPLPendingCleanup + 1})
	if err != nil {
		return nil, fmt.Errorf("creating pending ledger cleanup iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	result := make(map[string]uint64)

	for iter.First(); iter.Valid(); iter.Next() {
		// Key format: [zone][sub][ledger_name\x00]
		raw := iter.Key()[2:]
		ledgerName := string(raw[:len(raw)-1]) // strip null terminator

		value, valErr := iter.ValueAndErr()
		if valErr != nil {
			return nil, fmt.Errorf("reading pending cleanup value for %q: %w", ledgerName, valErr)
		}

		if len(value) < 8 {
			return nil, fmt.Errorf("invalid pending cleanup value for %q: expected 8 bytes, got %d", ledgerName, len(value))
		}

		result[ledgerName] = binary.BigEndian.Uint64(value[:8])
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("pending ledger cleanup iterator error: %w", err)
	}

	return result, nil
}
