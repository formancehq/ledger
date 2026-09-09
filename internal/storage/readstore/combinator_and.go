package readstore

// AndIterator implements merge-intersect of N sorted Iterators travelling in
// the same direction. It produces entities that appear in ALL children.
//
// The merge is direction-agnostic once "behind" and "ahead" are read from D's
// comparator instead of byte order, so one implementation serves both
// directions (EN-1966).
type AndIterator[D Direction] struct {
	children  []Iterator[D]
	cmp       func(a, b []byte) int
	current   []byte
	started   bool
	exhausted bool
	// onSkip, when non-nil, is invoked once per candidate discarded by the
	// converge loop (i.e. a row some child held but the intersection
	// rejected). Used by the query layer to attribute skip counts to
	// per-iterator stats.
	onSkip func()
}

func newAndIterator[D Direction](children []Iterator[D]) *AndIterator[D] {
	return &AndIterator[D]{children: children, cmp: comparator[D]()}
}

// NewAndIterator creates an ascending AND over the given children.
// At least two children are expected.
func NewAndIterator(children ...EntityIterator) *AndIterator[Asc] {
	return newAndIterator(children)
}

// NewReverseAndIterator creates a descending AND over the given children.
func NewReverseAndIterator(children ...ReverseIterator) *AndIterator[Desc] {
	return newAndIterator(children)
}

// SetOnSkip registers a callback fired each time the converge loop discards
// a candidate (a row held by some child that was not present in all others).
// Nil disables the callback.
func (it *AndIterator[D]) SetOnSkip(onSkip func()) {
	it.onSkip = onSkip
}

func (it *AndIterator[D]) Next() bool {
	if it.exhausted || len(it.children) == 0 {
		return false
	}

	// EVERY child is positioned before the first converge, rather than
	// advancing children[0] and letting converge seek the rest.
	//
	// The lazy form works ascending only by accident: an unpositioned child
	// returns an empty Current(), which sorts BELOW any candidate and so
	// reads as "behind, seek it forward" — the right verdict going up.
	// Descending, empty reads as "already past the end", converge adopts it
	// as the new candidate and the intersection collapses to nothing.
	// Positioning explicitly is correct in both directions and does not
	// depend on the sentinel at all.
	if !it.started {
		it.started = true

		for i := range it.children {
			if !it.children[i].Next() {
				it.exhausted = true

				return false
			}
		}

		return it.converge()
	}

	if !it.children[0].Next() {
		it.exhausted = true

		return false
	}

	return it.converge()
}

func (it *AndIterator[D]) Current() []byte {
	return it.current
}

func (it *AndIterator[D]) Seek(target []byte) bool {
	if len(it.children) == 0 {
		return false
	}

	// Absolute reposition: seek EVERY child to target. converge uses each
	// child's Current() as a candidate, so a child left at a stale position
	// past target would become the candidate and skip valid intersections
	// behind it (EN-1597). Clearing exhausted lets a re-seek after exhaustion
	// re-establish the intersection.
	it.exhausted = false
	it.started = true

	for i := range it.children {
		if !it.children[i].Seek(target) {
			it.exhausted = true

			return false
		}
	}

	return it.converge()
}

func (it *AndIterator[D]) Err() error {
	for _, child := range it.children {
		if err := child.Err(); err != nil {
			return err
		}
	}

	return nil
}

func (it *AndIterator[D]) Close() {
	for _, child := range it.children {
		child.Close()
	}
}

// converge finds the next entity that exists in all children.
// Assumes children[0] is already positioned at a valid entity.
func (it *AndIterator[D]) converge() bool {
	candidate := it.children[0].Current()

	for {
		allMatch := true

		for i := 1; i < len(it.children); i++ {
			cmp := it.cmp(it.children[i].Current(), candidate)

			if cmp < 0 {
				// Child is behind along the direction of travel — seek it on.
				if !it.children[i].Seek(candidate) {
					it.exhausted = true

					return false
				}

				cmp = it.cmp(it.children[i].Current(), candidate)
			}

			if cmp > 0 {
				// Child jumped past the candidate — adopt its value instead.
				candidate = it.children[i].Current()
				allMatch = false

				if !it.children[0].Seek(candidate) {
					it.exhausted = true

					return false
				}

				candidate = it.children[0].Current()

				break // restart the inner loop
			}
			// cmp == 0: this child matches, continue to next
		}

		if allMatch {
			// Own the buffer — candidate aliases a child's Pebble key memory,
			// invalidated by that child's next positioning call (#319).
			it.current = append(it.current[:0], candidate...)

			return true
		}

		if it.onSkip != nil {
			it.onSkip()
		}
	}
}

var (
	_ EntityIterator  = (*AndIterator[Asc])(nil)
	_ ReverseIterator = (*AndIterator[Desc])(nil)
)
