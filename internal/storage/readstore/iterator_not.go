package readstore

import "bytes"

// NotIterator implements merge-difference: universe \ child.
// It produces all entities from the universe iterator that do NOT appear
// in the child iterator.
type NotIterator struct {
	universe  EntityIterator
	child     EntityIterator
	childVal  []byte
	childDone bool
	current   []byte
	started   bool
	exhausted bool
}

// NewNotIterator creates a NOT iterator.
// universe is the full set of entities (e.g., existence index).
// child is the set to exclude.
func NewNotIterator(universe, child EntityIterator) *NotIterator {
	return &NotIterator{
		universe: universe,
		child:    child,
	}
}

func (it *NotIterator) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if it.child.Next() {
			it.childVal = it.child.Current()
		} else {
			it.childDone = true
		}
	}

	for it.universe.Next() {
		uv := it.universe.Current()

		// Advance child to catch up with universe
		for !it.childDone && bytes.Compare(it.childVal, uv) < 0 {
			if it.child.Next() {
				it.childVal = it.child.Current()
			} else {
				it.childDone = true
			}
		}

		// If child matches, skip this entity
		if !it.childDone && bytes.Equal(it.childVal, uv) {
			continue
		}

		it.current = uv

		return true
	}

	it.exhausted = true

	return false
}

func (it *NotIterator) Current() []byte {
	return it.current
}

func (it *NotIterator) SeekGE(target []byte) bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if it.child.SeekGE(target) {
			it.childVal = it.child.Current()
		} else {
			it.childDone = true
		}
	} else if !it.childDone {
		if it.child.SeekGE(target) {
			it.childVal = it.child.Current()
		} else {
			it.childDone = true
		}
	}

	if !it.universe.SeekGE(target) {
		it.exhausted = true

		return false
	}

	// Need to handle the case where universe lands on an excluded entity
	for {
		uv := it.universe.Current()

		for !it.childDone && bytes.Compare(it.childVal, uv) < 0 {
			if it.child.Next() {
				it.childVal = it.child.Current()
			} else {
				it.childDone = true
			}
		}

		if !it.childDone && bytes.Equal(it.childVal, uv) {
			if !it.universe.Next() {
				it.exhausted = true

				return false
			}

			continue
		}

		it.current = uv

		return true
	}
}

func (it *NotIterator) Close() {
	it.universe.Close()
	it.child.Close()
}
