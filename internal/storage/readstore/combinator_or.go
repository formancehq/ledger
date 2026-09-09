package readstore

import "bytes"

// OrIterator implements merge-union of N sorted Iterators travelling in the
// same direction. It produces entities that appear in ANY child, without
// duplicates.
//
// Ascending and descending differ only in which child is "leading" at each
// step — the minimum ascending, the maximum descending — so both are the same
// merge under D's comparator rather than two implementations (EN-1966).
type OrIterator[D Direction] struct {
	children  []Iterator[D]
	valid     []bool // tracks which children are still valid
	cmp       func(a, b []byte) int
	current   []byte
	started   bool
	exhausted bool
}

func newOrIterator[D Direction](children []Iterator[D]) *OrIterator[D] {
	return &OrIterator[D]{
		children: children,
		valid:    make([]bool, len(children)),
		cmp:      comparator[D](),
	}
}

// NewOrIterator creates an ascending OR over the given children.
func NewOrIterator(children ...EntityIterator) *OrIterator[Asc] {
	return newOrIterator(children)
}

// NewReverseOrIterator creates a descending OR over the given children.
func NewReverseOrIterator(children ...ReverseIterator) *OrIterator[Desc] {
	return newOrIterator(children)
}

func (it *OrIterator[D]) Next() bool {
	if it.exhausted || len(it.children) == 0 {
		return false
	}

	if !it.started {
		for i := range it.children {
			it.valid[i] = it.children[i].Next()
		}

		it.started = true

		return it.advanceToLeader()
	}

	// Advance every child currently sitting on the emitted entity — that is
	// what dedups the union.
	for i := range it.children {
		if it.valid[i] && bytes.Equal(it.children[i].Current(), it.current) {
			it.valid[i] = it.children[i].Next()
		}
	}

	return it.advanceToLeader()
}

func (it *OrIterator[D]) Current() []byte {
	return it.current
}

func (it *OrIterator[D]) Seek(target []byte) bool {
	if len(it.children) == 0 {
		return false
	}

	// Absolute reposition: clear the exhausted latch so a re-seek after
	// exhaustion re-establishes the union (all children are re-seeked below).
	it.exhausted = false

	for i := range it.children {
		it.valid[i] = it.children[i].Seek(target)
	}

	it.started = true

	return it.advanceToLeader()
}

func (it *OrIterator[D]) Err() error {
	for _, child := range it.children {
		if err := child.Err(); err != nil {
			return err
		}
	}

	return nil
}

func (it *OrIterator[D]) Close() {
	for _, child := range it.children {
		child.Close()
	}
}

// advanceToLeader adopts the current value of the child furthest along the
// direction of travel: the smallest entity ascending, the largest descending.
func (it *OrIterator[D]) advanceToLeader() bool {
	var leader []byte

	found := false

	for i := range it.children {
		if !it.valid[i] {
			continue
		}

		cur := it.children[i].Current()
		if !found || it.cmp(cur, leader) < 0 {
			leader = cur
			found = true
		}
	}

	if !found {
		it.exhausted = true

		return false
	}

	// Copy leader into our own buffer: child.Current() aliases the underlying
	// pebble.Iterator.Key() memory, which the next positioning call may
	// overwrite. On Next() we advance every child whose Current() equals
	// it.current; if it.current still pointed at the winning child's buffer
	// after that child advanced, the subsequent bytes.Equal comparisons
	// would run against rewritten memory and the duplicate-holding children
	// would not be advanced (resulting in duplicate or skipped rows in OR
	// queries — #319).
	it.current = append(it.current[:0], leader...)

	return true
}

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *OrIterator[D]) Direction() (d D) { return }

var (
	_ EntityIterator  = (*OrIterator[Asc])(nil)
	_ ReverseIterator = (*OrIterator[Desc])(nil)
)
