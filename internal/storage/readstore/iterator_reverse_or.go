package readstore

import "bytes"

// ReverseOrIterator implements merge-union of N ReverseIterators in descending
// order. It produces entities that appear in ANY child, without duplicates,
// yielding the maximum current value at each step.
type ReverseOrIterator struct {
	children []reverseChild
	current  []byte
	started  bool
	done     bool
}

type reverseChild struct {
	iter  ReverseIterator
	close func()
	valid bool
}

// NewReverseOrIterator creates a reverse-merge iterator over the given children.
// Each child must also have a Close() method.
func NewReverseOrIterator(children ...interface {
	ReverseIterator
	Close()
},
) *ReverseOrIterator {
	rc := make([]reverseChild, len(children))
	for i, c := range children {
		rc[i] = reverseChild{iter: c, close: c.Close}
	}

	return &ReverseOrIterator{children: rc}
}

func (it *ReverseOrIterator) Next() bool {
	if it.done || len(it.children) == 0 {
		return false
	}

	if !it.started {
		for i := range it.children {
			it.children[i].valid = it.children[i].iter.Next()
		}

		it.started = true

		return it.findMax()
	}

	// Advance all children currently at the max value.
	for i := range it.children {
		if it.children[i].valid && bytes.Equal(it.children[i].iter.Current(), it.current) {
			it.children[i].valid = it.children[i].iter.Next()
		}
	}

	return it.findMax()
}

func (it *ReverseOrIterator) Current() []byte {
	return it.current
}

func (it *ReverseOrIterator) SeekLE(target []byte) bool {
	if it.done || len(it.children) == 0 {
		return false
	}

	for i := range it.children {
		it.children[i].valid = it.children[i].iter.SeekLE(target)
	}

	it.started = true

	return it.findMax()
}

func (it *ReverseOrIterator) Close() {
	for _, c := range it.children {
		c.close()
	}
}

func (it *ReverseOrIterator) findMax() bool {
	var maxVal []byte

	found := false

	for i := range it.children {
		if !it.children[i].valid {
			continue
		}

		cur := it.children[i].iter.Current()
		if !found || bytes.Compare(cur, maxVal) > 0 {
			maxVal = cur
			found = true
		}
	}

	if !found {
		it.done = true

		return false
	}

	// Own the buffer — see iterator_or.go.findMin for the rationale.
	// child.Current() aliases the Pebble key memory and is invalidated on
	// the next positioning call (#319).
	it.current = append(it.current[:0], maxVal...)

	return true
}
