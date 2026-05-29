package query

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReadPendingLedgerCleanups reads all pending ledger cleanup entries from Pebble.
// Returns a map of ledger ID -> delete log sequence number.
func ReadPendingLedgerCleanups(reader dal.PebbleReader) (map[uint32]uint64, error) {
	iter, err := dal.NewBoundedIter(reader, []byte{dal.ZonePerLedger, dal.SubPLPendingCleanup}, []byte{dal.ZonePerLedger, dal.SubPLPendingCleanup + 1})
	if err != nil {
		return nil, fmt.Errorf("creating pending ledger cleanup iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	result := make(map[uint32]uint64)

	for iter.First(); iter.Valid(); iter.Next() {
		// Key format: [zone][sub][ledgerID_BE_4B]
		raw := iter.Key()[2:]
		if len(raw) < 4 {
			continue
		}

		ledgerID := binary.BigEndian.Uint32(raw[:4])

		value, valErr := iter.ValueAndErr()
		if valErr != nil {
			return nil, fmt.Errorf("reading pending cleanup value for ledger %d: %w", ledgerID, valErr)
		}

		if len(value) < 8 {
			return nil, fmt.Errorf("invalid pending cleanup value for ledger %d: expected 8 bytes, got %d", ledgerID, len(value))
		}

		result[ledgerID] = binary.BigEndian.Uint64(value[:8])
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("pending ledger cleanup iterator error: %w", err)
	}

	return result, nil
}
