package readstore

import bolt "go.etcd.io/bbolt"

// ReversePrefixIterator iterates over keys matching a prefix in descending
// order within a bbolt bucket. It extracts entity IDs from the key suffix.
// This is used for ListTransactions which returns newest-first (descending txID).
type ReversePrefixIterator struct {
	cursor       *bolt.Cursor
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
	cursor *bolt.Cursor,
	prefix []byte,
	entityOffset int,
	entityLen int,
) *ReversePrefixIterator {
	return &ReversePrefixIterator{
		cursor:       cursor,
		prefix:       prefix,
		entityOffset: entityOffset,
		entityLen:    entityLen,
	}
}

func (it *ReversePrefixIterator) Next() bool {
	if it.exhausted {
		return false
	}

	var k []byte
	if !it.started {
		it.started = true
		// Seek to the end of the prefix range, then step back
		upper := IncrementBytes(it.prefix)
		k, _ = it.cursor.Seek(upper)
		if k == nil {
			// Past the end of the bucket — go to last key
			k, _ = it.cursor.Last()
		} else {
			// Seek found a key >= upper, step back to last key in prefix range
			k, _ = it.cursor.Prev()
		}
	} else {
		k, _ = it.cursor.Prev()
	}

	for k != nil && HasPrefix(k, it.prefix) {
		entity := it.extractEntity(k)
		if entity != nil {
			it.current = entity
			return true
		}
		k, _ = it.cursor.Prev()
	}

	it.exhausted = true
	return false
}

func (it *ReversePrefixIterator) Current() []byte {
	return it.current
}

// SeekLE positions the iterator at the first entity whose key is <= target.
// Used for afterTxID pagination in descending order.
func (it *ReversePrefixIterator) SeekLE(target []byte) bool {
	if it.exhausted {
		return false
	}

	// Build seek key: prefix base + target entity
	seekKey := make([]byte, 0, it.entityOffset+len(target))
	seekKey = append(seekKey, it.prefix[:min(it.entityOffset, len(it.prefix))]...)
	seekKey = append(seekKey, target...)

	k, _ := it.cursor.Seek(seekKey)
	it.started = true

	// Seek positions at first key >= seekKey. If exact match, use it.
	// If past target, step back.
	if k == nil {
		// Past the end — go to last key in bucket
		k, _ = it.cursor.Last()
	} else {
		entity := it.extractEntity(k)
		if entity != nil && compareEntities(entity, target) <= 0 && HasPrefix(k, it.prefix) {
			it.current = entity
			return true
		}
		// Key is > target, step back
		k, _ = it.cursor.Prev()
	}

	for k != nil && HasPrefix(k, it.prefix) {
		entity := it.extractEntity(k)
		if entity != nil && compareEntities(entity, target) <= 0 {
			it.current = entity
			return true
		}
		k, _ = it.cursor.Prev()
	}

	it.exhausted = true
	return false
}

func (it *ReversePrefixIterator) Close() {}

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
