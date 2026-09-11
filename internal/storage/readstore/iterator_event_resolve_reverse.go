package readstore

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReverseEventResolveIterator is the descending twin of
// EventResolveIterator's POINT form: it reads a metadata / exists index event
// range at a pinned raft sequence and yields, in descending key order, every
// group whose latest event at or below the pin is an ADD (see event_keys.go).
//
// Direction changes only how a group is resolved, never the verdict. Events of
// one group are key-adjacent and seq-ASCENDING, so walking the group backwards
// visits them seq-descending and the FIRST event with seq <= pin is the latest
// one at or below the pin — exactly the event the forward pass keeps last.
// The scan still walks the group to its first key before moving on, because
// that is what positions the raw iterator on the preceding group.
//
// The range form is deliberately absent. Its groups are (encodedValue, entity)
// pairs that do not surface in entity order, so every construction site
// materializes it and serves both directions from the sorted slice — see
// ReverseSliceIterator and query.materializeIterator.
type ReverseEventResolveIterator struct {
	iter       *pebble.Iterator
	seekPrefix []byte // the scan prefix, prepended to seek targets
	prefixLen  int    // key bytes before the group identity
	pin        uint64

	current   []byte
	started   bool
	exhausted bool
	ceil      seekCeil
	err       error
}

// NewReverseEventResolveIterator scans the event range under prefix (built by
// MetadataIndexEventValuePrefixV or an EntityExists*PrefixV) as of pin,
// descending.
func NewReverseEventResolveIterator(reader dal.PebbleReader, prefix []byte, pin uint64) (*ReverseEventResolveIterator, error) {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: IncrementBytes(prefix),
	})
	if err != nil {
		return nil, err
	}

	return &ReverseEventResolveIterator{iter: iter, seekPrefix: prefix, prefixLen: len(prefix), pin: pin}, nil
}

// settleFrom resolves groups backwards from the raw iterator's position until
// one is live at the pin. seekTarget is the target of the seek that led here,
// or nil when advancing through Next: a scan that runs out while seeking
// proves no live group at or below that target, which the ceil memoises.
func (it *ReverseEventResolveIterator) settleFrom(seekTarget []byte) bool {
	live := it.settle()
	if !live && seekTarget != nil && it.err == nil {
		it.ceil.fail(seekTarget, it.iter.Error())
	}

	return live
}

// settle walks groups from high to low. Within a group it walks events from
// high seq to low, and the first event at or below the pin decides; the walk
// then continues to the group's first key so the raw iterator lands on the
// preceding group.
func (it *ReverseEventResolveIterator) settle() bool {
	for it.iter.Valid() {
		g, _, _, ok := parseEventKey(it.iter.Key(), it.prefixLen)
		if !ok {
			it.err = fmt.Errorf("malformed metadata event key %x", it.iter.Key())

			return false
		}

		group := append([]byte(nil), g...)
		live := false
		decided := false

		for it.iter.Valid() {
			g, seq, op, ok := parseEventKey(it.iter.Key(), it.prefixLen)
			if !ok {
				it.err = fmt.Errorf("malformed metadata event key %x", it.iter.Key())

				return false
			}

			if !bytes.Equal(g, group) {
				break
			}

			// Walking backwards, the first event at or below the pin is the
			// LATEST such event — the one the forward pass settles on.
			if !decided && seq <= it.pin {
				live = op == MetadataEventAdd
				decided = true
			}

			if !it.iter.Prev() {
				break
			}
		}

		if live {
			it.current = group

			return true
		}
	}

	it.exhausted = true

	return false
}

func (it *ReverseEventResolveIterator) Next() bool {
	if it.err != nil || it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if !it.iter.Last() {
			it.exhausted = true

			return false
		}
	}

	// After a successful settle the raw iterator already rests on the
	// preceding group's last key.
	return it.settleFrom(nil)
}

func (it *ReverseEventResolveIterator) Current() []byte { return it.current }

// Seek positions at the largest live group <= target.
//
// The seek bound is prefix+target+(terminator+1): every event key of group
// `target` is prefix+target+terminator+seq+op, so all of them sort below that
// bound, and any longer group starting with `target` sorts above it — entity
// bytes can never be NUL or 0x01 (account addresses are [a-zA-Z0-9:_-]+;
// transaction IDs are fixed-width, so no group is a strict prefix of another).
// SeekGE to that bound then Prev() therefore lands on the LAST event of group
// `target`, or on the last event of the largest group below it.
func (it *ReverseEventResolveIterator) Seek(target []byte) bool {
	if it.err != nil {
		return false
	}

	// A prior failed seek at or above target proves this one empty too.
	if it.ceil.covers(target) {
		it.exhausted = true

		return false
	}

	// Absolute reposition: clear the exhausted latch so a re-seek after
	// exhaustion still finds groups (the body re-seeks from target).
	it.exhausted = false
	it.started = true

	seekKey := make([]byte, 0, len(it.seekPrefix)+len(target)+1)
	seekKey = append(seekKey, it.seekPrefix...)
	seekKey = append(seekKey, target...)
	seekKey = append(seekKey, metadataEventTerminator+1)

	if it.iter.SeekGE(seekKey) {
		if !it.iter.Prev() {
			it.exhausted = true
			it.ceil.fail(target, it.iter.Error())

			return false
		}
	} else if !it.iter.Last() {
		// Nothing at or above the bound and the range is empty.
		it.exhausted = true
		it.ceil.fail(target, it.iter.Error())

		return false
	}

	return it.settleFrom(target)
}

func (it *ReverseEventResolveIterator) Err() error {
	if it.err != nil {
		return it.err
	}

	return it.iter.Error()
}

func (it *ReverseEventResolveIterator) Close() {
	_ = it.iter.Close()
}

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *ReverseEventResolveIterator) Direction() (d Desc) { return }

var _ ReverseIterator = (*ReverseEventResolveIterator)(nil)
