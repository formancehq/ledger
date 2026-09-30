package readstore

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// IDDateRangeIterator returns a builtin date range in the public entity-ID
// order. The ID-first projection lets a cursor seek directly to its next page.
// For an uncursored first page it finds the exact first/last matching ID in
// the date index with constant memory before walking the ID-first projection.
// Both views are acquired from the same read-store snapshot.
type IDDateRangeIterator[D Direction] struct {
	reader               dal.PebbleReader
	iter                 *pebble.Iterator
	idPrefix             []byte
	dateLower, dateUpper []byte
	dateEntityOffset     int
	lowerDate, upperDate uint64
	hasMin, hasMax       bool
	stampPin             uint64
	stamped              bool
	reverse              bool
	current              []byte
	firstID, lastID      []byte
	extremaKnown         bool
	unmatched            int
	started, exhausted   bool
	err                  error
}

// A cursor page normally walks directly to its next match. After this many
// unrelated IDs, consult the date index to bound a sparse or exhausted page.
const idDateUnmatchedLimit = 64

func NewIDDateRangeIterator[D Direction](
	reader dal.PebbleReader,
	idPrefix, dateLower, dateUpper []byte,
	dateEntityOffset int,
	lowerDate, upperDate uint64,
	hasMin, hasMax, stamped bool,
	stampPin uint64,
) (*IDDateRangeIterator[D], error) {
	iter, err := reader.NewIter(&pebble.IterOptions{LowerBound: idPrefix, UpperBound: IncrementBytes(idPrefix)})
	if err != nil {
		return nil, err
	}
	var direction D
	_, reverse := any(direction).(Desc)

	return &IDDateRangeIterator[D]{reader: reader, iter: iter, idPrefix: copyBytes(idPrefix), dateLower: copyBytes(dateLower), dateUpper: copyBytes(dateUpper), dateEntityOffset: dateEntityOffset, lowerDate: lowerDate, upperDate: upperDate, hasMin: hasMin, hasMax: hasMax, stamped: stamped, stampPin: stampPin, reverse: reverse}, nil
}

func (*IDDateRangeIterator[D]) Direction() (d D)   { return }
func (it *IDDateRangeIterator[D]) Current() []byte { return it.current }

func (it *IDDateRangeIterator[D]) Next() bool {
	if it.err != nil || it.exhausted {
		return false
	}
	switch {
	case !it.started:
		it.started = true
		first, last, id, ok := it.dateIDs(nil)
		if !ok {
			it.exhausted = true

			return false
		}
		it.firstID, it.lastID, it.extremaKnown = first, last, true
		it.seekID(id)
		if !it.iter.Valid() || !bytes.Equal(it.iter.Key()[len(it.idPrefix):], id) {
			if err := it.iter.Error(); err != nil {
				it.err = err
			} else {
				it.err = fmt.Errorf("invariant: date-first row %x has no ID-first companion", id)
			}

			return false
		}
		if !it.findMatch() {
			if it.err == nil && it.iter.Error() == nil {
				it.err = fmt.Errorf("invariant: date-first row %x is absent from ID-first range", id)
			}

			return false
		}
		if !bytes.Equal(it.current, id) {
			it.err = fmt.Errorf("invariant: date-first row %x disagrees with ID-first range", id)

			return false
		}

		return true
	case it.reverse:
		it.iter.Prev()
	default:
		it.iter.Next()
	}

	return it.findMatch()
}

func (it *IDDateRangeIterator[D]) Seek(target []byte) bool {
	if it.err != nil {
		return false
	}
	if len(target) != 8 {
		it.err = fmt.Errorf("invariant: date-range ID seek width %d, want 8", len(target))

		return false
	}
	it.started, it.exhausted, it.unmatched = true, false, 0
	it.seekID(target)

	return it.findMatch()
}

func (it *IDDateRangeIterator[D]) seekID(id []byte) {
	key := make([]byte, len(it.idPrefix)+8)
	copy(key, it.idPrefix)
	copy(key[len(it.idPrefix):], id)
	if it.iter.SeekGE(key) {
		if it.reverse && bytes.Compare(it.iter.Key(), key) > 0 {
			it.iter.Prev()
		}
	} else if it.reverse {
		it.iter.Last()
	}
}

func (it *IDDateRangeIterator[D]) findMatch() bool {
	for it.iter.Valid() {
		key, value := it.iter.Key(), it.iter.Value()
		if len(key) != len(it.idPrefix)+8 {
			it.err = fmt.Errorf("invariant: ID-date key suffix width %d, want 8", len(key)-len(it.idPrefix))

			return false
		}
		id := key[len(it.idPrefix):]
		if it.extremaKnown && ((it.reverse && bytes.Compare(id, it.firstID) < 0) || (!it.reverse && bytes.Compare(id, it.lastID) > 0)) {
			it.exhausted = true

			return false
		}
		wantValueLen := 8
		if it.stamped {
			wantValueLen = 16
		}
		if len(value) != wantValueLen {
			it.err = fmt.Errorf("invariant: ID-date value width %d, want %d", len(value), wantValueLen)

			return false
		}
		date := binary.BigEndian.Uint64(value[:8])
		if (!it.hasMin || date >= it.lowerDate) && (!it.hasMax || date < it.upperDate) && (!it.stamped || it.stampPin == 0 || binary.BigEndian.Uint64(value[8:]) <= it.stampPin) {
			it.current = copyBytes(key[len(it.idPrefix):])
			it.unmatched = 0

			return true
		}
		it.unmatched++
		if it.unmatched >= idDateUnmatchedLimit {
			first, last, next, ok := it.dateIDs(id)
			if !ok {
				it.exhausted = true

				return false
			}
			it.firstID, it.lastID, it.extremaKnown = first, last, true
			if next == nil {
				it.exhausted = true

				return false
			}
			it.seekID(next)
			if !it.iter.Valid() || !bytes.Equal(it.iter.Key()[len(it.idPrefix):], next) {
				if err := it.iter.Error(); err != nil {
					it.err = err
				} else {
					it.err = fmt.Errorf("invariant: date-first row %x has no ID-first companion", next)
				}

				return false
			}

			continue
		}
		if it.reverse {
			it.iter.Prev()
		} else {
			it.iter.Next()
		}
	}
	it.exhausted = true

	return false
}

// dateIDs reports the matching extrema and the next match beyond after in the
// iterator's direction. The latter lets sparse cursor pages skip an unrelated
// ID gap without materializing the date-range matches.
func (it *IDDateRangeIterator[D]) dateIDs(after []byte) ([]byte, []byte, []byte, bool) {
	rangeIter, err := NewStampGatedRangeIterator(it.reader, it.dateLower, it.dateUpper, it.dateEntityOffset, 8, it.stampPin)
	if err != nil {
		it.err = err

		return nil, nil, nil, false
	}
	defer rangeIter.Close()
	var first, last, next []byte
	for rangeIter.Next() {
		candidate := rangeIter.Current()
		if first == nil || bytes.Compare(candidate, first) < 0 {
			first = append(first[:0], candidate...)
		}
		if last == nil || bytes.Compare(candidate, last) > 0 {
			last = append(last[:0], candidate...)
		}
		if after != nil && ((it.reverse && bytes.Compare(candidate, after) < 0) || (!it.reverse && bytes.Compare(candidate, after) > 0)) &&
			(next == nil || (it.reverse && bytes.Compare(candidate, next) > 0) || (!it.reverse && bytes.Compare(candidate, next) < 0)) {
			next = append(next[:0], candidate...)
		}
	}
	if err := rangeIter.Err(); err != nil {
		it.err = err

		return nil, nil, nil, false
	}
	if after == nil {
		next = first
		if it.reverse {
			next = last
		}
	}

	return first, last, next, first != nil
}

func (it *IDDateRangeIterator[D]) Err() error {
	if it.err != nil {
		return it.err
	}

	return it.iter.Error()
}

func (it *IDDateRangeIterator[D]) Close() { _ = it.iter.Close() }
