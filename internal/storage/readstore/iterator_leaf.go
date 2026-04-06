package readstore

import (
	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// PrefixIterator scans all keys in the read index Pebble database that share a
// given prefix, extracting the entity ID from the suffix portion of each key.
type PrefixIterator struct {
	iter         *pebble.Iterator
	prefix       []byte
	entityOffset int // byte offset where the entity ID starts in each key
	entityLen    int // fixed entity length (0 = variable, extends to end of key)
	current      []byte
	started      bool
	exhausted    bool
}

// NewPrefixIterator creates an iterator that scans all keys with the given
// prefix. entityOffset is the byte position where the entity ID starts.
// entityLen is 0 for variable-length entities (accounts) or 8 for fixed-length (txIDs).
// The caller provides a PebbleReader (snapshot or DB).
func NewPrefixIterator(
	reader dal.PebbleReader,
	prefix []byte,
	entityOffset int,
	entityLen int,
) (*PrefixIterator, error) {
	upper := IncrementBytes(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}

	return &PrefixIterator{
		iter:         iter,
		prefix:       prefix,
		entityOffset: entityOffset,
		entityLen:    entityLen,
	}, nil
}

func (it *PrefixIterator) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if !it.iter.First() {
			it.exhausted = true

			return false
		}

		entity := it.extractEntity(it.iter.Key())
		if entity != nil {
			it.current = entity

			return true
		}
	}

	for it.iter.Next() {
		entity := it.extractEntity(it.iter.Key())
		if entity != nil {
			it.current = entity

			return true
		}
	}

	it.exhausted = true

	return false
}

func (it *PrefixIterator) Current() []byte {
	return it.current
}

func (it *PrefixIterator) SeekGE(target []byte) bool {
	if it.exhausted {
		return false
	}

	// Build a seek key: prefix base + target entity.
	seekKey := make([]byte, 0, it.entityOffset+len(target))
	seekKey = append(seekKey, it.prefix[:min(it.entityOffset, len(it.prefix))]...)
	seekKey = append(seekKey, target...)

	it.started = true

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true

		return false
	}

	// The Pebble iterator is bounded by UpperBound, so no need to check HasPrefix.
	for it.iter.Valid() {
		entity := it.extractEntity(it.iter.Key())
		if entity != nil && compareEntities(entity, target) >= 0 {
			it.current = entity

			return true
		}

		if !it.iter.Next() {
			break
		}
	}

	it.exhausted = true

	return false
}

func (it *PrefixIterator) Close() {
	if it.iter != nil {
		_ = it.iter.Close()
	}
}

func (it *PrefixIterator) extractEntity(key []byte) []byte {
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

// RangeIterator scans keys between lower and upper bounds in the read index,
// extracting entity IDs from each key.
type RangeIterator struct {
	iter         *pebble.Iterator
	entityOffset int
	entityLen    int
	current      []byte
	started      bool
	exhausted    bool
}

// NewRangeIterator creates an iterator that scans keys in [lower, upper).
func NewRangeIterator(
	reader dal.PebbleReader,
	lower, upper []byte,
	entityOffset int,
	entityLen int,
) (*RangeIterator, error) {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}

	return &RangeIterator{
		iter:         iter,
		entityOffset: entityOffset,
		entityLen:    entityLen,
	}, nil
}

func (it *RangeIterator) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if !it.iter.First() {
			it.exhausted = true

			return false
		}

		entity := it.extractEntity(it.iter.Key())
		if entity != nil {
			it.current = entity

			return true
		}
	}

	for it.iter.Next() {
		entity := it.extractEntity(it.iter.Key())
		if entity != nil {
			it.current = entity

			return true
		}
	}

	it.exhausted = true

	return false
}

func (it *RangeIterator) Current() []byte {
	return it.current
}

func (it *RangeIterator) SeekGE(target []byte) bool {
	if it.exhausted {
		return false
	}

	seekKey := make([]byte, 0, it.entityOffset+len(target))
	// Use the current iterator key prefix if available, otherwise use lower bound.
	if it.iter.Valid() {
		k := it.iter.Key()
		if len(k) >= it.entityOffset {
			seekKey = append(seekKey, k[:it.entityOffset]...)
		}
	}

	seekKey = append(seekKey[:min(len(seekKey), it.entityOffset)], target...)

	it.started = true

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true

		return false
	}

	for it.iter.Valid() {
		entity := it.extractEntity(it.iter.Key())
		if entity != nil && compareEntities(entity, target) >= 0 {
			it.current = entity

			return true
		}

		if !it.iter.Next() {
			break
		}
	}

	it.exhausted = true

	return false
}

func (it *RangeIterator) Close() {
	if it.iter != nil {
		_ = it.iter.Close()
	}
}

func (it *RangeIterator) extractEntity(key []byte) []byte {
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
