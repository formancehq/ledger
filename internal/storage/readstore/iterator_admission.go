package readstore

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// admitFoldStamp applies the same visibility predicate in both scan directions.
// Callers latch malformed-value errors and exhaust their iterator.
func admitFoldStamp(iter *pebble.Iterator, pin uint64) (bool, error) {
	if pin == 0 {
		return true, nil
	}

	value := iter.Value()
	if len(value) != 8 {
		return false, fmt.Errorf("stamp-gated scan: row %x carries a %d-byte value (want an 8-byte fold sequence)", iter.Key(), len(value))
	}

	return binary.BigEndian.Uint64(value) <= pin, nil
}
