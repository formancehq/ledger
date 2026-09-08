package readstore

import (
	"bytes"
	"sort"
)

// ReverseSliceIterator presents a pre-sorted (ascending) slice of entity IDs
// as a descending ReverseIterator. It is the descending counterpart of
// SliceIterator: materializing fallback leaves (value-ordered ranges, the
// address→transaction union) keep their single sorted slice, and this view
// traverses it high-to-low without building a second all-result collection.
type ReverseSliceIterator struct {
	entities [][]byte
	pos      int // index of the entity the next Next() yields
	current  []byte
	closeFn  func()
}

// NewReverseSliceIterator creates a descending view over entities, which must
// already be sorted in ascending byte order and must not be mutated afterwards.
func NewReverseSliceIterator(entities [][]byte) *ReverseSliceIterator {
	return &ReverseSliceIterator{entities: entities, pos: len(entities) - 1}
}

// SetClose registers a cleanup callback run by Close. Materializing owners
// (e.g. the address iterator whose underlying account iterator otherwise
// would leak) pass their Close method here.
func (it *ReverseSliceIterator) SetClose(closeFn func()) {
	it.closeFn = closeFn
}

func (it *ReverseSliceIterator) Next() bool {
	if it.pos < 0 || it.pos >= len(it.entities) {
		return false
	}

	it.current = it.entities[it.pos]
	it.pos--

	return true
}

func (it *ReverseSliceIterator) Current() []byte {
	return it.current
}

// SeekLE positions the iterator at the largest entity <= target. It mirrors
// SliceIterator.SeekGE: the result is computed from target alone, so repeated
// and backward seeks behave as absolute repositions of a descending view.
func (it *ReverseSliceIterator) SeekLE(target []byte) bool {
	idx := sort.Search(len(it.entities), func(i int) bool {
		return bytes.Compare(it.entities[i], target) > 0
	}) - 1
	if idx < 0 {
		it.pos = -1

		return false
	}

	it.current = it.entities[idx]
	it.pos = idx - 1

	return true
}

func (it *ReverseSliceIterator) Err() error { return nil }

func (it *ReverseSliceIterator) Close() {
	if it.closeFn != nil {
		it.closeFn()
	}
}

var _ ReverseIterator = (*ReverseSliceIterator)(nil)
