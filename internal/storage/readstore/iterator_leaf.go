package readstore

import bolt "go.etcd.io/bbolt"

// PrefixIterator scans all keys in a bbolt bucket that share a given prefix,
// extracting the entity ID from the suffix portion of each key.
type PrefixIterator struct {
	cursor       *bolt.Cursor
	prefix       []byte
	entityOffset int // byte offset where the entity ID starts in each key
	entityLen    int // fixed entity length (0 = variable, extends to end of key)
	current      []byte
	started      bool
	exhausted    bool
}

// NewPrefixIterator creates an iterator that scans all keys with the given
// prefix in the specified bucket. entityOffset is the byte position where the
// entity ID starts (typically len(prefix) for forward index keys where value
// encoding is part of the prefix, or after value encoding for metadata index).
// entityLen is 0 for variable-length entities (accounts) or 8 for fixed-length (txIDs).
func NewPrefixIterator(
	cursor *bolt.Cursor,
	prefix []byte,
	entityOffset int,
	entityLen int,
) *PrefixIterator {
	return &PrefixIterator{
		cursor:       cursor,
		prefix:       prefix,
		entityOffset: entityOffset,
		entityLen:    entityLen,
	}
}

func (it *PrefixIterator) Next() bool {
	if it.exhausted {
		return false
	}

	var k []byte
	if !it.started {
		k, _ = it.cursor.Seek(it.prefix)
		it.started = true
	} else {
		k, _ = it.cursor.Next()
	}

	for k != nil && HasPrefix(k, it.prefix) {
		entity := it.extractEntity(k)
		if entity != nil {
			it.current = entity
			return true
		}
		k, _ = it.cursor.Next()
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

	// Build a seek key: prefix + target (for metadata index keys where entity
	// is the suffix). For existence index, the entity IS the suffix of the key.
	seekKey := make([]byte, 0, it.entityOffset+len(target))
	seekKey = append(seekKey, it.prefix[:min(it.entityOffset, len(it.prefix))]...)
	seekKey = append(seekKey, target...)

	k, _ := it.cursor.Seek(seekKey)
	it.started = true

	for k != nil && HasPrefix(k, it.prefix) {
		entity := it.extractEntity(k)
		if entity != nil && compareEntities(entity, target) >= 0 {
			it.current = entity
			return true
		}
		k, _ = it.cursor.Next()
	}

	it.exhausted = true
	return false
}

func (it *PrefixIterator) Close() {}

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

// RangeIterator scans keys between lower and upper bounds in a bbolt bucket,
// extracting entity IDs from each key.
type RangeIterator struct {
	cursor       *bolt.Cursor
	lower        []byte
	upper        []byte // exclusive upper bound
	entityOffset int
	entityLen    int
	current      []byte
	started      bool
	exhausted    bool
}

// NewRangeIterator creates an iterator that scans keys in [lower, upper).
func NewRangeIterator(
	cursor *bolt.Cursor,
	lower, upper []byte,
	entityOffset int,
	entityLen int,
) *RangeIterator {
	return &RangeIterator{
		cursor:       cursor,
		lower:        lower,
		upper:        upper,
		entityOffset: entityOffset,
		entityLen:    entityLen,
	}
}

func (it *RangeIterator) Next() bool {
	if it.exhausted {
		return false
	}

	var k []byte
	if !it.started {
		k, _ = it.cursor.Seek(it.lower)
		it.started = true
	} else {
		k, _ = it.cursor.Next()
	}

	for k != nil {
		if it.upper != nil && compareEntities(k, it.upper) >= 0 {
			break
		}
		entity := it.extractEntity(k)
		if entity != nil {
			it.current = entity
			return true
		}
		k, _ = it.cursor.Next()
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
	seekKey = append(seekKey, it.lower[:min(it.entityOffset, len(it.lower))]...)
	seekKey = append(seekKey, target...)

	k, _ := it.cursor.Seek(seekKey)
	it.started = true

	for k != nil {
		if it.upper != nil && compareEntities(k, it.upper) >= 0 {
			break
		}
		entity := it.extractEntity(k)
		if entity != nil && compareEntities(entity, target) >= 0 {
			it.current = entity
			return true
		}
		k, _ = it.cursor.Next()
	}

	it.exhausted = true
	return false
}

func (it *RangeIterator) Close() {}

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
