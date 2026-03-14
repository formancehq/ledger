package readstore

import (
	"github.com/cockroachdb/pebble"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReversePrefixIterator iterates over keys matching a prefix in descending
// order within the read index. It extracts entity IDs from the key suffix.
type ReversePrefixIterator struct {
	iter         *pebble.Iterator
	prefix       []byte
	entityOffset int
	entityLen    int
	current      []byte
	started      bool
	exhausted    bool
}

// NewReversePrefixIterator creates an iterator that scans all keys with the
// given prefix in reverse order. entityOffset is the byte position where the
// entity ID starts, entityLen is 0 for variable-length or 8 for fixed-length.
func NewReversePrefixIterator(
	reader dal.PebbleReader,
	prefix []byte,
	entityOffset int,
	entityLen int,
) (*ReversePrefixIterator, error) {
	upper := IncrementBytes(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}

	return &ReversePrefixIterator{
		iter:         iter,
		prefix:       prefix,
		entityOffset: entityOffset,
		entityLen:    entityLen,
	}, nil
}

func (it *ReversePrefixIterator) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if !it.iter.Last() {
			it.exhausted = true

			return false
		}

		entity := it.extractEntity(it.iter.Key())
		if entity != nil {
			it.current = entity

			return true
		}
	}

	for it.iter.Prev() {
		entity := it.extractEntity(it.iter.Key())
		if entity != nil {
			it.current = entity

			return true
		}
	}

	it.exhausted = true

	return false
}

func (it *ReversePrefixIterator) Current() []byte {
	return it.current
}

// SeekLE positions the iterator at the first entity whose key is <= target.
func (it *ReversePrefixIterator) SeekLE(target []byte) bool {
	if it.exhausted {
		return false
	}

	// Build seek key: prefix base + target entity
	seekKey := make([]byte, 0, it.entityOffset+len(target))
	seekKey = append(seekKey, it.prefix[:min(it.entityOffset, len(it.prefix))]...)
	seekKey = append(seekKey, target...)

	it.started = true

	// SeekGE positions at first key >= seekKey. If exact match, check it.
	// If past target, step back.
	if it.iter.SeekGE(seekKey) {
		entity := it.extractEntity(it.iter.Key())
		if entity != nil && compareEntities(entity, target) <= 0 {
			it.current = entity

			return true
		}
		// Key is > target, step back
		if !it.iter.Prev() {
			it.exhausted = true

			return false
		}
	} else if !it.iter.Last() {
		// Past the end — go to last key
		it.exhausted = true

		return false
	}

	for it.iter.Valid() {
		entity := it.extractEntity(it.iter.Key())
		if entity != nil && compareEntities(entity, target) <= 0 {
			it.current = entity

			return true
		}

		if !it.iter.Prev() {
			break
		}
	}

	it.exhausted = true

	return false
}

func (it *ReversePrefixIterator) Close() {
	if it.iter != nil {
		_ = it.iter.Close()
	}
}

func (it *ReversePrefixIterator) extractEntity(key []byte) []byte {
	if len(key) <= it.entityOffset {
		return nil
	}

	suffix := key[it.entityOffset:]
	if it.entityLen > 0 {
		if len(suffix) < it.entityLen {
			return nil
		}

		return suffix[:it.entityLen]
	}

	return suffix
}

// IncrementBytes increments a byte slice by 1 (treating as big-endian unsigned
// integer). Returns nil on overflow (all 0xFF).
func IncrementBytes(b []byte) []byte {
	result := make([]byte, len(b))
	copy(result, b)

	for i := len(result) - 1; i >= 0; i-- {
		result[i]++
		if result[i] != 0 {
			return result
		}
	}
	// Overflow
	return append(result, 0xFF)
}
