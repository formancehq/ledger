package query

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/bitset"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadReversions loads all per-ledger reversion bitsets from Pebble.
// Key format: [0xE5][ledger\x00][wordIndex BE 8 bytes] → [uint64 LE 8 bytes].
func ReadReversions(reader dal.PebbleReader) (map[string]*bitset.Bitset, error) {
	lowerBound := []byte{dal.KeyPrefixReversions}
	upperBound := []byte{dal.KeyPrefixReversions + 1}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating reversions iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	result := make(map[string]*bitset.Bitset)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		// Key format: [0xE5][ledger\x00][wordIndex BE 8 bytes]
		// Find the null separator after the ledger name.
		nullIdx := bytes.IndexByte(key[1:], 0x00)
		if nullIdx < 0 || len(key) < 1+nullIdx+1+8 {
			continue
		}

		ledger := string(key[1 : 1+nullIdx])
		wordIndex := binary.BigEndian.Uint64(key[1+nullIdx+1:])

		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading reversion word for %s: %w", ledger, err)
		}

		if len(val) < 8 {
			continue
		}

		word := binary.LittleEndian.Uint64(val)

		bs, ok := result[ledger]
		if !ok {
			bs = &bitset.Bitset{}
			result[ledger] = bs
		}

		bs.SetWord(wordIndex, word)
	}

	return result, nil
}
