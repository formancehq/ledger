package readstore

import "sort"

// SliceIterator walks a caller-owned, ascending-sorted slice of entity IDs in
// D's direction. The slice is BORROWED, never copied: a materializing leaf
// hands the same sorted result to either direction, so serving a descending
// page from an already materialized range costs no second collection
// (EN-1966).
//
// The caller owns the slice and must not mutate it for the iterator's
// lifetime. Entries must be sorted ascending, since Seek binary-searches
// them.
//
// It lives here rather than in the query package so both directions share one
// implementation; the query package previously owned the ascending half while
// readstore owned the descending one.
type SliceIterator[D Direction] struct {
	entities [][]byte
	cmp      func(a, b []byte) int
	// step is the travel increment over TRAVEL positions, which run 0..n-1
	// in the direction of travel; at() maps them onto slice indices.
	step      int
	pos       int
	started   bool
	exhausted bool
	current   []byte
}

func newSliceIterator[D Direction](entities [][]byte) *SliceIterator[D] {
	return &SliceIterator[D]{entities: entities, cmp: comparator[D](), step: travelStep[D]()}
}

// NewSliceIterator borrows entities (ascending, sorted) and walks them low to
// high. A nil slice yields an empty iterator.
func NewSliceIterator(entities [][]byte) *SliceIterator[Asc] {
	return newSliceIterator[Asc](entities)
}

// NewReverseSliceIterator borrows entities (ascending, sorted) and walks them
// high to low.
func NewReverseSliceIterator(entities [][]byte) *SliceIterator[Desc] {
	return newSliceIterator[Desc](entities)
}

// at maps a travel position onto a slice index. Travel position 0 is the
// first entity in the direction of travel.
func (it *SliceIterator[D]) at(travel int) []byte {
	if it.step > 0 {
		return it.entities[travel]
	}

	return it.entities[len(it.entities)-1-travel]
}

func (it *SliceIterator[D]) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		it.pos = 0
	} else {
		it.pos++
	}

	if it.pos >= len(it.entities) {
		it.exhausted = true

		return false
	}

	it.current = it.at(it.pos)

	return true
}

func (it *SliceIterator[D]) Current() []byte { return it.current }

// Seek positions at the first entity at or after target in D's order: the
// smallest entity >= target ascending, the largest entity <= target
// descending. It is computed from target alone, so it is idempotent,
// non-consuming at the same target, and well-defined after exhaustion — the
// absolute-seek contract in iterator.go.
func (it *SliceIterator[D]) Seek(target []byte) bool {
	// cmp(at(i), target) is monotone non-decreasing over travel positions in
	// either direction, so one binary search serves both.
	idx := sort.Search(len(it.entities), func(i int) bool {
		return it.cmp(it.at(i), target) >= 0
	})
	if idx >= len(it.entities) {
		it.exhausted = true

		return false
	}

	it.exhausted = false
	it.started = true
	it.pos = idx
	it.current = it.at(idx)

	return true
}

// Err is always nil: the slice is already in memory, so there is no storage
// read left to fail. The leaf that produced it checked its own Err before
// handing the slice over — see query.materializeIterator.
func (it *SliceIterator[D]) Err() error { return nil }

func (it *SliceIterator[D]) Close() {}

// Slice borrows entities (ascending, sorted) and walks them in D's direction.
// See And for why both a generic and a named spelling exist.
func Slice[D Direction](entities [][]byte) *SliceIterator[D] {
	return newSliceIterator[D](entities)
}

// travelStep is +1 ascending and -1 descending: the increment over slice
// indices that moves along the direction of travel.
func travelStep[D Direction]() int {
	var d D
	if d.compare([]byte{0}, []byte{1}) < 0 {
		return 1
	}

	return -1
}

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *SliceIterator[D]) Direction() (d D) { return }

var (
	_ EntityIterator  = (*SliceIterator[Asc])(nil)
	_ ReverseIterator = (*SliceIterator[Desc])(nil)
)
