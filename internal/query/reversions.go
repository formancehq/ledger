package query

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadReversions loads all per-ledger reversion bitsets from Pebble.
func ReadReversions(reader dal.PebbleReader) (map[string]*domain.ReversionBitset, error) {
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

	result := make(map[string]*domain.ReversionBitset)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		// Key format: [KeyPrefixReversions][ledger_name]
		ledger := string(key[1:])

		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading reversions for %s: %w", ledger, err)
		}

		result[ledger] = domain.ReversionBitsetFromWords(val)
	}

	return result, nil
}
