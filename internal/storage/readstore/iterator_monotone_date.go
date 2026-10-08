package readstore

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// MonotoneDateIterator streams a date-first index whose dates are guaranteed
// nondecreasing with entity IDs. Equal dates are ordered by the ID suffix.
// Seek accepts an ID, so it scans from the current position when possible;
// an absolute/backward seek restarts at the date bound. No companion index is
// needed, and the first page reads only its own rows plus lookahead.
type MonotoneDateIterator[D Direction] struct {
	iter         *pebble.Iterator
	lower        []byte
	entityOffset int
	reverse      bool
	current      []byte
	started      bool
	exhausted    bool
	err          error
}

func NewMonotoneDateIterator[D Direction](reader dal.PebbleReader, lower, upper []byte, entityOffset int) (*MonotoneDateIterator[D], error) {
	lower, upper = copyBytes(lower), copyBytes(upper)
	iter, err := reader.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	var direction D
	_, reverse := any(direction).(Desc)

	return &MonotoneDateIterator[D]{iter: iter, lower: lower, entityOffset: entityOffset, reverse: reverse}, nil
}

func (*MonotoneDateIterator[D]) Direction() (d D)   { return }
func (it *MonotoneDateIterator[D]) Current() []byte { return it.current }

func (it *MonotoneDateIterator[D]) positionFirst() bool {
	if it.reverse {
		return it.iter.Last()
	}

	return it.iter.SeekGE(it.lower)
}

func (it *MonotoneDateIterator[D]) advance() bool {
	if it.reverse {
		return it.iter.Prev()
	}

	return it.iter.Next()
}

func (it *MonotoneDateIterator[D]) setCurrent() bool {
	if !it.iter.Valid() {
		it.exhausted = true
		it.current = nil

		return false
	}
	key := it.iter.Key()
	if len(key) != it.entityOffset+8 {
		it.err = fmt.Errorf("invariant: log date key suffix width %d, want 8", len(key)-it.entityOffset)

		return false
	}
	it.current = copyBytes(key[it.entityOffset:])

	return true
}

func (it *MonotoneDateIterator[D]) Next() bool {
	if it.err != nil || it.exhausted {
		return false
	}
	if !it.started {
		it.started = true
		it.positionFirst()
	} else {
		it.advance()
	}

	return it.setCurrent()
}

func (it *MonotoneDateIterator[D]) Seek(target []byte) bool {
	if it.err != nil {
		return false
	}
	if len(target) != 8 {
		it.err = fmt.Errorf("invariant: log date ID seek width %d, want 8", len(target))

		return false
	}
	// A forward seek can continue from the current date key. AND/OR may also
	// seek backwards, in which case the only safe position is the date bound.
	if !it.started || it.exhausted || it.current == nil ||
		(!it.reverse && bytes.Compare(target, it.current) < 0) ||
		(it.reverse && bytes.Compare(target, it.current) > 0) {
		it.positionFirst()
	}
	it.started, it.exhausted = true, false
	for it.iter.Valid() {
		if !it.setCurrent() {
			return false
		}
		cmp := bytes.Compare(it.current, target)
		if (!it.reverse && cmp >= 0) || (it.reverse && cmp <= 0) {
			return true
		}
		it.advance()
	}
	it.exhausted = true
	it.current = nil

	return false
}

func (it *MonotoneDateIterator[D]) Err() error {
	if it.err != nil {
		return it.err
	}

	return it.iter.Error()
}
func (it *MonotoneDateIterator[D]) Close() { _ = it.iter.Close() }
