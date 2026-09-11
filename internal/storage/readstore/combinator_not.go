package readstore

import "bytes"

// NotIterator implements merge-difference: universe \ child, in the shared
// direction of both operands.
//
// Nothing about the difference is direction-specific once "child is behind
// universe" is read from D's comparator, so one implementation serves both
// directions (EN-1966).
type NotIterator[D Direction] struct {
	universe  Iterator[D]
	child     Iterator[D]
	cmp       func(a, b []byte) int
	childVal  []byte
	childDone bool
	current   []byte
	started   bool
	exhausted bool
}

func newNotIterator[D Direction](universe, child Iterator[D]) *NotIterator[D] {
	return &NotIterator[D]{universe: universe, child: child, cmp: comparator[D]()}
}

// NewNotIterator creates an ascending NOT.
// universe is the full set of entities (e.g. existence index); child is the
// set to exclude.
func NewNotIterator(universe, child EntityIterator) *NotIterator[Asc] {
	return newNotIterator[Asc](universe, child)
}

// NewReverseNotIterator creates a descending NOT.
func NewReverseNotIterator(universe, child ReverseIterator) *NotIterator[Desc] {
	return newNotIterator[Desc](universe, child)
}

// catchUpChild advances child until it is no longer behind uv.
func (it *NotIterator[D]) catchUpChild(uv []byte) {
	for !it.childDone && it.cmp(it.childVal, uv) < 0 {
		if it.child.Next() {
			it.childVal = it.child.Current()
		} else {
			it.childDone = true
		}
	}
}

func (it *NotIterator[D]) Next() bool {
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

		it.catchUpChild(uv)

		if !it.childDone && bytes.Equal(it.childVal, uv) {
			continue
		}

		it.current = uv

		return true
	}

	it.exhausted = true

	return false
}

func (it *NotIterator[D]) Current() []byte {
	return it.current
}

func (it *NotIterator[D]) Seek(target []byte) bool {
	// Absolute reposition: clear the exhausted latch so a re-seek after
	// exhaustion still finds entities (the universe is re-seeked below).
	it.exhausted = false

	it.started = true

	// Re-position the child on every Seek, including after it has reported
	// done. Next() iteration can consume the child past a later seek target
	// (nested NOTs reach this: the outer NOT advances the inner one to emit
	// its first row, exhausting a finite child such as the reversion
	// bitset). A latched childDone would then leave the child unable to
	// report that the entity at target is excluded, leaking it into the
	// difference. Seek is absolute repositioning, so re-seeking is always
	// well-defined (EN-1597).
	if it.child.Seek(target) {
		it.childVal = it.child.Current()
		it.childDone = false
	} else {
		it.childDone = true
	}

	if !it.universe.Seek(target) {
		it.exhausted = true

		return false
	}

	// The universe may land on an excluded entity; step it on until it does
	// not.
	for {
		uv := it.universe.Current()

		it.catchUpChild(uv)

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

func (it *NotIterator[D]) Err() error {
	if err := it.universe.Err(); err != nil {
		return err
	}

	return it.child.Err()
}

func (it *NotIterator[D]) Close() {
	it.universe.Close()
	it.child.Close()
}

// Not creates a NOT over operands travelling in D's direction. See And for
// why both a generic and a named spelling exist.
func Not[D Direction](universe, child Iterator[D]) *NotIterator[D] {
	return newNotIterator[D](universe, child)
}

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *NotIterator[D]) Direction() (d D) { return }

var (
	_ EntityIterator  = (*NotIterator[Asc])(nil)
	_ ReverseIterator = (*NotIterator[Desc])(nil)
)
