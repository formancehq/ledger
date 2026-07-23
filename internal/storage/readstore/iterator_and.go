package readstore

import "bytes"

// AndIterator implements merge-intersect of N sorted EntityIterators.
// It produces entities that appear in ALL child iterators.
type AndIterator struct {
	children  []EntityIterator
	current   []byte
	exhausted bool
	// onSkip, when non-nil, is invoked once per candidate discarded by the
	// converge loop (i.e. a row some child held but the intersection rejected).
	// Used by the query layer to attribute skip counts to per-iterator stats.
	onSkip func()
}

// NewAndIterator creates a new AND iterator over the given children.
// At least two children are required.
func NewAndIterator(children ...EntityIterator) *AndIterator {
	return &AndIterator{children: children}
}

// SetOnSkip registers a callback fired each time the converge loop discards
// a candidate (a row held by some child that was not present in all others).
// Nil disables the callback.
func (it *AndIterator) SetOnSkip(onSkip func()) {
	it.onSkip = onSkip
}

func (it *AndIterator) Next() bool {
	if it.exhausted || len(it.children) == 0 {
		return false
	}

	// Advance first child
	if !it.children[0].Next() {
		it.exhausted = true

		return false
	}

	return it.converge()
}

func (it *AndIterator) Current() []byte {
	return it.current
}

func (it *AndIterator) SeekGE(target []byte) bool {
	if len(it.children) == 0 {
		return false
	}

	// Absolute reposition: seek EVERY child to target, not just children[0].
	// converge uses each child's Current() as a candidate, so a child left at a
	// stale position past target would become the candidate and skip valid
	// intersections below it. Clearing exhausted lets a re-seek after exhaustion
	// re-establish the intersection.
	it.exhausted = false

	for i := range it.children {
		if !it.children[i].SeekGE(target) {
			it.exhausted = true

			return false
		}
	}

	return it.converge()
}

func (it *AndIterator) Err() error {
	for _, child := range it.children {
		if err := child.Err(); err != nil {
			return err
		}
	}

	return nil
}

func (it *AndIterator) Close() {
	for _, child := range it.children {
		child.Close()
	}
}

// converge finds the next entity that exists in all children.
// Assumes children[0] is already positioned at a valid entity.
func (it *AndIterator) converge() bool {
	candidate := it.children[0].Current()

	for {
		allMatch := true

		for i := 1; i < len(it.children); i++ {
			cmp := bytes.Compare(it.children[i].Current(), candidate)

			if cmp < 0 {
				// Child is behind — seek forward
				if !it.children[i].SeekGE(candidate) {
					it.exhausted = true

					return false
				}

				cmp = bytes.Compare(it.children[i].Current(), candidate)
			}

			if cmp > 0 {
				// Child jumped ahead — use its value as the new candidate
				candidate = it.children[i].Current()
				allMatch = false

				// Seek the first child to the new candidate
				if !it.children[0].SeekGE(candidate) {
					it.exhausted = true

					return false
				}

				candidate = it.children[0].Current()

				break // restart the inner loop
			}
			// cmp == 0: this child matches, continue to next
		}

		if allMatch {
			it.current = candidate

			return true
		}

		if it.onSkip != nil {
			it.onSkip()
		}
	}
}
