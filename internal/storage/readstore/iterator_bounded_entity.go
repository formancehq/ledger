package readstore

import (
	"fmt"

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
	err       error
}

// NewBoundedEntityIterator scans unique fixed-width entity suffixes in the
// half-open [lower, upper) range. The prefix contains every byte before the
// entity, so its length determines the extraction offset. Nil bounds are open;
// an open upper bound uses the prefix successor to include the maximum entity.
// entityLen must be positive; non-nil bounds must have exactly that width.
func NewBoundedEntityIterator(reader dal.PebbleReader, prefix, lower, upper []byte, entityLen int) (*BoundedEntityIterator, error) {
	if entityLen <= 0 {
		return nil, fmt.Errorf("invariant: BoundedEntityIterator entityLen must be positive, got %d", entityLen)
	}
	if lower != nil && len(lower) != entityLen {
		return nil, fmt.Errorf("invariant: BoundedEntityIterator lower bound length %d, want %d", len(lower), entityLen)
	}
	if upper != nil && len(upper) != entityLen {
		return nil, fmt.Errorf("invariant: BoundedEntityIterator upper bound length %d, want %d", len(upper), entityLen)
	}

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
	if it.err != nil || it.exhausted {
		return false
	}
	var valid bool
	if !it.started {
		it.started = true
		valid = it.iter.SeekGE(it.lowerBound)
	} else {
		valid = it.iter.Next()
	}
	if !valid {
		it.exhausted = true

		return false
	}

	return it.readCurrent()
}

func (*BoundedEntityIterator) Direction() (d Asc) { return }

func (it *BoundedEntityIterator) Current() []byte { return it.current }

func (it *BoundedEntityIterator) Seek(target []byte) bool {
	if it.err != nil {
		return false
	}
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

	return it.readCurrent()
}

func (it *BoundedEntityIterator) Err() error {
	if it.err != nil {
		return it.err
	}
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

// readCurrent enforces the unique fixed-width suffix contract before emission.
// A malformed row poisons the iterator; it cannot prove clean exhaustion or be
// skipped in favor of a plausible but incomplete result.
func (it *BoundedEntityIterator) readCurrent() bool {
	key := it.iter.Key()
	suffixLen := len(key) - it.idOffset
	if suffixLen != it.entityLen {
		it.err = fmt.Errorf("invariant: BoundedEntityIterator key suffix length %d, want %d", suffixLen, it.entityLen)
		it.current = nil
		it.exhausted = true
		it.floor = seekFloor{}

		return false
	}
	it.current = copyBytes(key[it.idOffset:])

	return true
}
