package readstore

import "bytes"

// ReverseNotIterator implements merge-difference in descending order:
// universe \ child. It produces all entities from the descending universe
// iterator that do NOT appear in the descending child iterator — the
// descending mirror of NotIterator.
type ReverseNotIterator struct {
	universe      ReverseIterator
	child         ReverseIterator
	universeClose func()
	childClose    func()
	childVal      []byte
	childDone     bool
	current       []byte
	started       bool
	exhausted     bool
}

// NewReverseNotIterator creates a descending NOT iterator. universe is the
// full set of entities (descending); child is the excluded set (descending).
func NewReverseNotIterator(universe, child interface {
	ReverseIterator
	Close()
},
) *ReverseNotIterator {
	return &ReverseNotIterator{
		universe:      universe,
		child:         child,
		universeClose: universe.Close,
		childClose:    child.Close,
	}
}

func (it *ReverseNotIterator) Next() bool {
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

		// Advance the (descending) child DOWN to catch up with the universe.
		for !it.childDone && bytes.Compare(it.childVal, uv) > 0 {
			if it.child.Next() {
				it.childVal = it.child.Current()
			} else {
				it.childDone = true
			}
		}

		if !it.childDone && bytes.Equal(it.childVal, uv) {
			continue
		}

		it.current = uv

		return true
	}

	it.exhausted = true

	return false
}

func (it *ReverseNotIterator) Current() []byte {
	return it.current
}

func (it *ReverseNotIterator) SeekLE(target []byte) bool {
	it.exhausted = false
	it.started = true

	// Re-position the child on every seek, mirroring NotIterator.SeekGE — a
	// latched childDone would leak excluded entities back into the difference.
	if it.child.SeekLE(target) {
		it.childVal = it.child.Current()
		it.childDone = false
	} else {
		it.childDone = true
	}

	if !it.universe.SeekLE(target) {
		it.exhausted = true

		return false
	}

	for {
		uv := it.universe.Current()

		for !it.childDone && bytes.Compare(it.childVal, uv) > 0 {
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

func (it *ReverseNotIterator) Err() error {
	if err := it.universe.Err(); err != nil {
		return err
	}

	return it.child.Err()
}

func (it *ReverseNotIterator) Close() {
	it.universeClose()
	it.childClose()
}
