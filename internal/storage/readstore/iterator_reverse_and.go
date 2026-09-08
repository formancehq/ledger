package readstore

import "bytes"

// ReverseAndIterator implements merge-intersect of N ReverseIterators in
// descending order. It produces entities that appear in ALL child iterators —
// the descending mirror of AndIterator.
type ReverseAndIterator struct {
	children  []ReverseIterator
	closers   []func()
	current   []byte
	started   bool
	exhausted bool
	onSkip    func()
}

// NewReverseAndIterator creates a descending AND iterator over the given
// children. At least two children are required. Each child must also expose a
// Close method (the composite's Close delegates to every child).
func NewReverseAndIterator(children ...interface {
	ReverseIterator
	Close()
},
) *ReverseAndIterator {
	out := &ReverseAndIterator{}
	for _, c := range children {
		out.children = append(out.children, c)
		out.closers = append(out.closers, c.Close)
	}

	return out
}

// SetOnSkip registers a callback fired each time the converge loop discards a
// candidate, mirroring AndIterator.SetOnSkip for profile attribution.
func (it *ReverseAndIterator) SetOnSkip(onSkip func()) {
	it.onSkip = onSkip
}

func (it *ReverseAndIterator) Next() bool {
	if it.exhausted || len(it.children) == 0 {
		return false
	}

	if !it.started {
		// Initialize every child — unlike the ascending AndIterator, which can
		// rely on the "unpositioned child reports nil (smallest)" accident to
		// route it into the seek-forward branch, a nil Current() here would be
		// misread as "child is below the candidate" in the descending merge.
		for i := range it.children {
			if !it.children[i].Next() {
				it.exhausted = true

				return false
			}
		}

		it.started = true
	} else {
		// Advance every child currently at the matched value.
		for i := range it.children {
			if bytes.Equal(it.children[i].Current(), it.current) {
				if !it.children[i].Next() {
					it.exhausted = true

					return false
				}
			}
		}
	}

	return it.converge()
}

func (it *ReverseAndIterator) Current() []byte {
	return it.current
}

// SeekLE repositions every child to the largest entity <= target, the
// descending mirror of AndIterator.SeekGE: a child left at a stale (larger)
// position would become the candidate and skip valid intersections above it.
func (it *ReverseAndIterator) SeekLE(target []byte) bool {
	if len(it.children) == 0 {
		return false
	}

	it.exhausted = false
	it.started = true

	for i := range it.children {
		if !it.children[i].SeekLE(target) {
			it.exhausted = true

			return false
		}
	}

	return it.converge()
}

func (it *ReverseAndIterator) Err() error {
	for _, child := range it.children {
		if err := child.Err(); err != nil {
			return err
		}
	}

	return nil
}

func (it *ReverseAndIterator) Close() {
	for _, close := range it.closers {
		close()
	}
}

// converge finds the next entity present in all children, descending. All
// children are already positioned at a valid entity.
func (it *ReverseAndIterator) converge() bool {
	candidate := it.children[0].Current()

	for {
		allMatch := true

		for i := 1; i < len(it.children); i++ {
			cmp := bytes.Compare(it.children[i].Current(), candidate)

			if cmp > 0 {
				// Child is ahead (larger) — bring it down to the candidate.
				if !it.children[i].SeekLE(candidate) {
					it.exhausted = true

					return false
				}

				cmp = bytes.Compare(it.children[i].Current(), candidate)
			}

			if cmp < 0 {
				// Child landed below the candidate — the candidate cannot be
				// present in every child; lower it to this child's value.
				candidate = it.children[i].Current()
				allMatch = false

				if !it.children[0].SeekLE(candidate) {
					it.exhausted = true

					return false
				}

				candidate = it.children[0].Current()

				break
			}
			// cmp == 0: matched in this child, continue to next.
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
