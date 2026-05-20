package query

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/bitset"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadReversions loads all per-ledger reversion bitsets from Pebble.
// Key format: [0x03][0x01][ledger\x00][wordIndex BE 8 bytes] → [uint64 LE 8 bytes].
func ReadReversions(reader dal.PebbleReader) (map[uint32]*bitset.Bitset, error) {
	lowerBound := []byte{dal.ZonePerLedger, dal.SubPLReversions}
	upperBound := []byte{dal.ZonePerLedger, dal.SubPLReversions + 1}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating reversions iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	result := make(map[uint32]*bitset.Bitset)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		// Key format: [0x03][0x01][ledgerID_BE_4B][wordIndex BE 8 bytes]
		if len(key) < 2+4+8 {
			continue
		}

		ledgerID := binary.BigEndian.Uint32(key[2:6])
		wordIndex := binary.BigEndian.Uint64(key[6:])

		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading reversion word for ledger %d: %w", ledgerID, err)
		}

		if len(val) < 8 {
			continue
		}

		word := binary.LittleEndian.Uint64(val)

		bs, ok := result[ledgerID]
		if !ok {
			bs = &bitset.Bitset{}
			result[ledgerID] = bs
		}

		bs.SetWord(wordIndex, word)
	}

	return result, nil
}
