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
	if it.exhausted || len(it.children) == 0 {
		return false
	}

	for i := range it.children {
		it.valid[i] = it.children[i].SeekGE(target)
	}
	it.started = true

	return it.findMin()
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

	it.current = minVal
	return true
}
