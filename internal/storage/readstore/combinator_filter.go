package readstore

// FilterIterator yields the entities of inner admitted by keep, in inner's
// direction. Positioning preserves the absolute-seek contract (see
// iterator.go and iterator-seek-contract.md): every operation delegates to
// inner and then steps over rejected entities, so a seek lands on the first
// ADMITTED entity at or after target and repeated or backward seeks
// reposition like the inner iterator does.
//
// keep may fail (it typically probes another store); a failure latches like a
// storage error — iteration stops and the error surfaces through Err,
// matching the Iterator contract's sticky-error convention.
//
// The type is direction-parameterized rather than duplicated: skipping
// rejected rows is "keep calling Next", which is already direction-relative,
// so there is nothing left for a second implementation to get wrong
// (EN-1966).
type FilterIterator[D Direction] struct {
	inner Iterator[D]
	keep  func(entity []byte) (bool, error)
	err   error
}

// NewFilterIterator wraps an ascending inner so only entities admitted by
// keep surface.
func NewFilterIterator(inner EntityIterator, keep func(entity []byte) (bool, error)) *FilterIterator[Asc] {
	return &FilterIterator[Asc]{inner: inner, keep: keep}
}

// NewFilterReverseIterator wraps a descending inner so only entities admitted
// by keep surface.
func NewFilterReverseIterator(inner ReverseIterator, keep func(entity []byte) (bool, error)) *FilterIterator[Desc] {
	return &FilterIterator[Desc]{inner: inner, keep: keep}
}

// settle advances inner until it rests on an admitted entity. ok is the
// result of the positioning call that preceded it.
func (it *FilterIterator[D]) settle(ok bool) bool {
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

func (it *FilterIterator[D]) Next() bool {
	if it.err != nil {
		return false
	}

	return it.settle(it.inner.Next())
}

func (it *FilterIterator[D]) Seek(target []byte) bool {
	if it.err != nil {
		return false
	}

	return it.settle(it.inner.Seek(target))
}

func (it *FilterIterator[D]) Current() []byte { return it.inner.Current() }

func (it *FilterIterator[D]) Err() error {
	if it.err != nil {
		return it.err
	}

	return it.inner.Err()
}

// Close releases inner. The wrapper owns the iterator it was handed, so
// callers must close the WRAPPER and never the raw inner iterator — closing
// past the wrapper bypasses that ownership and would leak any resource a
// future wrapper acquires of its own.
func (it *FilterIterator[D]) Close() { it.inner.Close() }

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *FilterIterator[D]) Direction() (d D) { return }

var (
	_ EntityIterator  = (*FilterIterator[Asc])(nil)
	_ ReverseIterator = (*FilterIterator[Desc])(nil)
)
