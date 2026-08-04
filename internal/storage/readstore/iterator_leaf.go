package readstore

import (
	"errors"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
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
	floor        seekFloor
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
		// SeekPrefixGE enables bloom filter checks: Pebble skips SSTables
		// whose bloom filter does not contain the prefix extracted by
		// Comparer.Split (the ledger-scoped prefix). This applies to
		// the initial seek and all subsequent Next() calls.
		if !it.iter.SeekPrefixGE(it.prefix) {
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
	// A prior failed seek at or below target proves this one empty too.
	if it.floor.covers(target) {
		it.exhausted = true

		return false
	}

	// Absolute reposition: clear the exhausted latch so a re-seek after
	// exhaustion still finds entities (the body re-seeks from target).
	it.exhausted = false

	// Build a seek key: prefix base + target entity.
	seekKey := make([]byte, 0, it.entityOffset+len(target))
	seekKey = append(seekKey, it.prefix[:min(it.entityOffset, len(it.prefix))]...)
	seekKey = append(seekKey, target...)

	it.started = true

	if !it.iter.SeekPrefixGE(seekKey) {
		it.exhausted = true
		it.floor.fail(target, it.iter.Error())

		return false
	}

	// SeekPrefixGE constrains iteration to the prefix; UpperBound is still respected.
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
	it.floor.fail(target, it.iter.Error())

	return false
}

func (it *PrefixIterator) Err() error {
	if it.iter == nil {
		return nil
	}

	return it.iter.Error()
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
// extracting entity IDs from each key. When the range spans several
// index-value buckets the emitted entities are NOT globally sorted — they
// surface in (value, entity) order — so this iterator only supports forward
// draining; see SeekGE.
type RangeIterator struct {
	iter         *pebble.Iterator
	lowerBound   []byte // stored for SeekPrefixGE initial positioning
	entityOffset int
	entityLen    int
	current      []byte
	started      bool
	exhausted    bool
	seekErr      error
}

// errInvariantRangeIteratorSeek fails a query that composed a RangeIterator
// without materializing it first — see RangeIterator.SeekGE.
var errInvariantRangeIteratorSeek = errors.New("invariant: RangeIterator.SeekGE called; materialize into a SliceIterator before composing")

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
		lowerBound:   lower,
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
		if !it.iter.SeekPrefixGE(it.lowerBound) {
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

// SeekGE cannot be implemented on a raw range scan: the [lower, upper) range
// may span several index-value buckets, so rows surface in (value, entity)
// order and "the first entity >= target" is undefined without draining the
// whole range. Every construction site materializes this iterator into a
// sorted SliceIterator before composing (internal/query.materializeIterator),
// which serves seeks over the sorted result; a call here is an invariant
// violation and fails the query loudly instead of returning
// plausible-looking wrong rows.
func (it *RangeIterator) SeekGE([]byte) bool {
	it.seekErr = errInvariantRangeIteratorSeek
	it.exhausted = true

	return false
}

func (it *RangeIterator) Err() error {
	if it.seekErr != nil {
		return it.seekErr
	}

	if it.iter == nil {
		return nil
	}

	return it.iter.Error()
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
