package query

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// ReadReversions loads all per-ledger reversion bitsets from Pebble.
// Key format: [0x03][0x01][ledgerName padded 64B][wordIndex BE 8 bytes] → [uint64 LE 8 bytes].
func ReadReversions(reader dal.PebbleReader) (map[string]*bitset.Bitset, error) {
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

	result := make(map[string]*bitset.Bitset)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		// Key format: [0x03][0x01][ledgerName padded 64B][wordIndex BE 8 bytes].
		if len(key) < 2+dal.LedgerNameFixedSize+8 {
			continue
		}

		nameBytes := key[2 : 2+dal.LedgerNameFixedSize]

		end := bytes.IndexByte(nameBytes, 0)
		if end < 0 {
			end = dal.LedgerNameFixedSize
		}

		ledgerName := string(nameBytes[:end])
		wordIndex := binary.BigEndian.Uint64(key[2+dal.LedgerNameFixedSize:])

		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading reversion word for ledger %q: %w", ledgerName, err)
		}

		if len(val) < 8 {
			continue
		}

		word := binary.LittleEndian.Uint64(val)

		bs, ok := result[ledgerName]
		if !ok {
			bs = &bitset.Bitset{}
			result[ledgerName] = bs
		}

		bs.SetWord(wordIndex, word)
	}

	return result, nil
}

// ReadReversionBitset loads a single ledger's reversion bitset from Pebble.
// It scans only that ledger's words rather than every ledger's (unlike
// ReadReversions) and returns a never-nil bitset — empty when the ledger has no
// reversions.
func ReadReversionBitset(reader dal.PebbleReader, ledgerName string) (*bitset.Bitset, error) {
	prefix := make([]byte, 2+dal.LedgerNameFixedSize)
	prefix[0] = dal.ZonePerLedger
	prefix[1] = dal.SubPLReversions
	copy(prefix[2:], ledgerName)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: readstore.IncrementBytes(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("creating reversion iterator for ledger %q: %w", ledgerName, err)
	}

	defer func() { _ = iter.Close() }()

	bs := &bitset.Bitset{}

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 2+dal.LedgerNameFixedSize+8 {
			continue
		}

		wordIndex := binary.BigEndian.Uint64(key[2+dal.LedgerNameFixedSize:])

		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading reversion word for ledger %q: %w", ledgerName, err)
		}

		if len(val) < 8 {
			continue
		}

		bs.SetWord(wordIndex, binary.LittleEndian.Uint64(val))
	}

	return bs, nil
}
