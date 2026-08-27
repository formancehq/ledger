package readstore

// FilterIterator yields the entities of inner admitted by keep. Positioning
// preserves the absolute SeekGE contract (see iterator-seek-contract.md):
// every operation delegates to inner and then skips forward over rejected
// entities, so a seek lands on the first ADMITTED entity >= target and
// repeated or backward seeks reposition like the inner iterator does.
//
// keep may fail (it typically probes another store); a failure latches like a
// storage error — iteration stops and the error surfaces through Err, matching
// the EntityIterator contract's sticky-error convention.
type FilterIterator struct {
	inner EntityIterator
	keep  func(entity []byte) (bool, error)
	err   error
}

// NewFilterIterator wraps inner so only entities admitted by keep surface.
func NewFilterIterator(inner EntityIterator, keep func(entity []byte) (bool, error)) *FilterIterator {
	return &FilterIterator{inner: inner, keep: keep}
}

// settle advances inner until it rests on an admitted entity. ok is the
// result of the positioning call that preceded it.
func (it *FilterIterator) settle(ok bool) bool {
	for ok {
		admit, err := it.keep(it.inner.Current())
		if err != nil {
			it.err = err

			return false
		}

		if admit {
			return true
		}

		ok = it.inner.Next()
	}

	return false
}

func (it *FilterIterator) Next() bool {
	if it.err != nil {
		return false
	}

	return it.settle(it.inner.Next())
}

func (it *FilterIterator) SeekGE(target []byte) bool {
	if it.err != nil {
		return false
	}

	return it.settle(it.inner.SeekGE(target))
}

func (it *FilterIterator) Current() []byte { return it.inner.Current() }

func (it *FilterIterator) Err() error {
	if it.err != nil {
		return it.err
	}

	return it.inner.Err()
}

func (it *FilterIterator) Close() { it.inner.Close() }

// FilterReverseIterator is the descending counterpart of FilterIterator: it
// yields the entities of inner admitted by keep, with the same absolute-seek
// and sticky-error conventions mirrored onto SeekLE.
type FilterReverseIterator struct {
	inner ReverseIterator
	keep  func(entity []byte) (bool, error)
	err   error
}

// NewFilterReverseIterator wraps inner so only entities admitted by keep
// surface.
func NewFilterReverseIterator(inner ReverseIterator, keep func(entity []byte) (bool, error)) *FilterReverseIterator {
	return &FilterReverseIterator{inner: inner, keep: keep}
}

func (it *FilterReverseIterator) settle(ok bool) bool {
	for ok {
		admit, err := it.keep(it.inner.Current())
		if err != nil {
			it.err = err

			return false
		}

		if admit {
			return true
		}

		ok = it.inner.Next()
	}

	return false
}

func (it *FilterReverseIterator) Next() bool {
	if it.err != nil {
		return false
	}

	return it.settle(it.inner.Next())
}

func (it *FilterReverseIterator) SeekLE(target []byte) bool {
	if it.err != nil {
		return false
	}

	return it.settle(it.inner.SeekLE(target))
}

func (it *FilterReverseIterator) Current() []byte { return it.inner.Current() }

func (it *FilterReverseIterator) Err() error {
	if it.err != nil {
		return it.err
	}

	return it.inner.Err()
}
