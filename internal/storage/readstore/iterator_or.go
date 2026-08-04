package readstore

import "bytes"

// OrIterator implements merge-union of N sorted EntityIterators.
// It produces entities that appear in ANY child iterator, without duplicates.
type OrIterator struct {
	children  []EntityIterator
	valid     []bool // tracks which children are still valid
	current   []byte
	started   bool
	exhausted bool
}

// NewOrIterator creates a new OR iterator over the given children.
func NewOrIterator(children ...EntityIterator) *OrIterator {
	return &OrIterator{
		children: children,
		valid:    make([]bool, len(children)),
	}
}

func (it *OrIterator) Next() bool {
	if it.exhausted || len(it.children) == 0 {
		return false
	}

	if !it.started {
		// Initialize all children
		for i := range it.children {
			it.valid[i] = it.children[i].Next()
		}

		it.started = true

		return it.findMin()
	}

	// Advance all children that are currently at the minimum value
	for i := range it.children {
		if it.valid[i] && bytes.Equal(it.children[i].Current(), it.current) {
			it.valid[i] = it.children[i].Next()
		}
	}

	return it.findMin()
}

func (it *OrIterator) Current() []byte {
	return it.current
}

func (it *OrIterator) SeekGE(target []byte) bool {
	if len(it.children) == 0 {
		return false
	}

	// Absolute reposition: clear the exhausted latch so a re-seek after
	// exhaustion re-establishes the union (all children are re-seeked below).
	it.exhausted = false

	for i := range it.children {
		it.valid[i] = it.children[i].SeekGE(target)
	}

	it.started = true

	return it.findMin()
}

func (it *OrIterator) Err() error {
	for _, child := range it.children {
		if err := child.Err(); err != nil {
			return err
		}
	}

	return nil
}

func (it *OrIterator) Close() {
	for _, child := range it.children {
		child.Close()
	}
}

// findMin finds the minimum current value among all valid children.
func (it *OrIterator) findMin() bool {
	var minVal []byte

	found := false

	for i := range it.children {
		if !it.valid[i] {
			continue
		}

		cur := it.children[i].Current()
		if !found || bytes.Compare(cur, minVal) < 0 {
			minVal = cur
			found = true
		}
	}

	if !found {
		it.exhausted = true

		return false
	}

	// Copy minVal into our own buffer: child.Current() aliases the underlying
	// pebble.Iterator.Key() memory, which the next positioning call may
	// overwrite. On Next() we advance every child whose Current() equals
	// it.current; if it.current still pointed at the winning child's buffer
	// after that child advanced, the subsequent bytes.Equal comparisons
	// would run against rewritten memory and the duplicate-holding children
	// would not be advanced (resulting in duplicate or skipped rows in OR
	// queries — #319).
	it.current = append(it.current[:0], minVal...)

	return true
}
