package readstore

import (
	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// BoundedEntityIterator streams a prefix whose keys are ordered by a unique,
// fixed-width entity suffix. It shares bounds and absolute seek semantics across
// log and transaction ranges. It does not deduplicate multi-key entities.
type BoundedEntityIterator struct {
	iter       *pebble.Iterator
	prefix     []byte
	lowerBound []byte
	idOffset   int // len(prefix)
	entityLen  int

	current   []byte
	started   bool
	exhausted bool
	floor     seekFloor
}

// NewBoundedEntityIterator scans unique fixed-width entity suffixes in the
// half-open [lower, upper) range. The prefix contains every byte before the
// entity, so its length determines the extraction offset. Nil bounds are open;
// an open upper bound uses the prefix successor to include the maximum entity.
func NewBoundedEntityIterator(reader dal.PebbleReader, prefix, lower, upper []byte, entityLen int) (*BoundedEntityIterator, error) {
	lowerBound := make([]byte, len(prefix)+len(lower))
	copy(lowerBound, prefix)
	copy(lowerBound[len(prefix):], lower)

	var upperBound []byte
	if upper != nil {
		upperBound = make([]byte, len(prefix)+len(upper))
		copy(upperBound, prefix)
		copy(upperBound[len(prefix):], upper)
	} else {
		upperBound = IncrementBytes(prefix)
	}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}

	return &BoundedEntityIterator{
		iter:       iter,
		prefix:     copyBytes(prefix),
		lowerBound: lowerBound,
		idOffset:   len(prefix),
		entityLen:  entityLen,
	}, nil
}

func (it *BoundedEntityIterator) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if !it.iter.SeekGE(it.lowerBound) {
			it.exhausted = true

			return false
		}

		if entity := it.extractEntity(it.iter.Key()); entity != nil {
			it.current = copyBytes(entity)

			return true
		}
	}

	for it.iter.Next() {
		if entity := it.extractEntity(it.iter.Key()); entity != nil {
			it.current = copyBytes(entity)

			return true
		}
	}

	it.exhausted = true

	return false
}

func (it *BoundedEntityIterator) Current() []byte { return it.current }

func (it *BoundedEntityIterator) SeekGE(target []byte) bool {
	// A prior failed seek at or below target proves this one empty too.
	if it.floor.covers(target) {
		it.exhausted = true

		return false
	}

	// Absolute reposition: clear the exhausted latch so a re-seek after
	// exhaustion still finds entities (the body re-seeks from target).
	it.exhausted = false
	it.started = true

	seekKey := make([]byte, it.idOffset+len(target))
	copy(seekKey, it.prefix)
	copy(seekKey[it.idOffset:], target)

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true
		it.floor.fail(target, it.iter.Error())

		return false
	}

	entity := it.extractEntity(it.iter.Key())
	if entity != nil && compareEntities(entity, target) >= 0 {
		it.current = copyBytes(entity)

		return true
	}

	it.exhausted = true
	it.floor.fail(target, it.iter.Error())

	return false
}

func (it *BoundedEntityIterator) Err() error {
	if it.iter == nil {
		return nil
	}

	return it.iter.Error()
}

func (it *BoundedEntityIterator) Close() {
	if it.iter != nil {
		_ = it.iter.Close() // Best effort cleanup; iteration errors are reported by Err.
	}
}

func (it *BoundedEntityIterator) extractEntity(key []byte) []byte {
	if len(key) < it.idOffset+it.entityLen {
		return nil
	}

	return key[it.idOffset : it.idOffset+it.entityLen]
}
