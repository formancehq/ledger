package readstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// EventResolveIterator reads a metadata / exists index event range at a
// pinned raft sequence, yielding — in key order — every group whose latest
// event at or below the pin is an ADD (see event_keys.go).
//
// Events of one group are key-adjacent and seq-ascending, so resolution is a
// single forward pass: within a group, the last event with seq <= pin
// decides; events above the pin are invisible to this reader by
// construction. Dead groups (DEL or nothing at the pin) are skipped whole.
//
// Two shapes share the implementation:
//
//   - point form (NewEventResolveIterator): the prefix runs through the
//     encoded value (or nullFlag), so a group IS an entity and Current()
//     yields it directly. The absolute SeekGE contract
//     (iterator-seek-contract.md) holds: a seek positions at the first event
//     key of the first group whose entity >= target and resolves forward.
//   - range form (NewEventResolveRangeIterator): the bounds span several
//     values under a shared prefix, a group is (encodedValue, entity), and
//     Current() yields the entity by stripping the fixed-width value
//     (emitOffset). Entities are NOT emitted in entity order — the caller
//     must materialize + sort, and SeekGE fails loudly, mirroring
//     RangeIterator.
//
// TODO(EN-1748): add a seekFloor exhaustion cache before this leaf serves
// hot query paths.
type EventResolveIterator struct {
	iter       *pebble.Iterator
	seekPrefix []byte // point form: the scan prefix, prepended to seek targets
	prefixLen  int    // key bytes before the group identity
	emitOffset int    // group bytes to strip when emitting the entity
	pin        uint64
	rangeMode  bool

	current   []byte
	started   bool
	exhausted bool
	floor     seekFloor
	err       error
}

// errInvariantEventRangeSeek fails a query that composed a range-form event
// iterator without materializing it first — see NewEventResolveRangeIterator.
var errInvariantEventRangeSeek = errors.New("invariant: EventResolveIterator range form SeekGE called; materialize into a SliceIterator before composing")

// NewEventResolveIterator scans the event range under prefix (built by
// MetadataIndexEventValuePrefixV or an EntityExists*PrefixV) as of pin.
func NewEventResolveIterator(reader dal.PebbleReader, prefix []byte, pin uint64) (*EventResolveIterator, error) {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: IncrementBytes(prefix),
	})
	if err != nil {
		return nil, err
	}

	return &EventResolveIterator{iter: iter, seekPrefix: prefix, prefixLen: len(prefix), pin: pin}, nil
}

// NewEventResolveRangeIterator scans events across the value range
// [lower, upper) as of pin. prefixLen is the length of the shared prefix up
// to the encoded values; emitOffset is the fixed width of one encoded value
// (type tag + payload), stripped from the group to yield the entity.
func NewEventResolveRangeIterator(reader dal.PebbleReader, lower, upper []byte, prefixLen, emitOffset int, pin uint64) (*EventResolveIterator, error) {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}

	return &EventResolveIterator{iter: iter, prefixLen: prefixLen, emitOffset: emitOffset, pin: pin, rangeMode: true}, nil
}

// parse splits an event key into (group, seq, op). The terminator position
// is computed from the right — the fixed suffix makes it unambiguous. ok is
// false for anything this package would not have written, unknown ops
// included: the caller turns that into a loud error rather than resolving a
// group from bytes it cannot read.
func (it *EventResolveIterator) parse(key []byte) (group []byte, seq uint64, op byte, ok bool) {
	rest := key[it.prefixLen:]
	tpos := len(rest) - metadataEventSuffixLen - 1
	if tpos < 0 || rest[tpos] != metadataEventTerminator {
		return nil, 0, 0, false
	}

	if op := rest[tpos+9]; !validEventOp(op) {
		return nil, 0, 0, false
	}

	return rest[:tpos], binary.BigEndian.Uint64(rest[tpos+1 : tpos+9]), rest[tpos+9], true
}

// settle resolves consecutive groups starting at the raw iterator's current
// position until one is live at the pin, leaving the raw iterator at the
// following group.
// settle resolves consecutive groups from the raw iterator's current position
// until one is live at the pin. seekTarget is the target of the seek that led
// here, or nil when advancing through Next: a scan that runs out while seeking
// proves no live group at or beyond that target, which the floor memoises.
func (it *EventResolveIterator) settleFrom(seekTarget []byte) bool {
	live := it.settle()
	if !live && seekTarget != nil && it.err == nil {
		it.floor.fail(seekTarget, it.iter.Error())
	}

	return live
}

func (it *EventResolveIterator) settle() bool {
	for it.iter.Valid() {
		g, _, _, ok := it.parse(it.iter.Key())
		if !ok {
			it.err = fmt.Errorf("malformed metadata event key %x", it.iter.Key())

			return false
		}

		group := append([]byte(nil), g...)
		live := false

		for it.iter.Valid() {
			g, seq, op, ok := it.parse(it.iter.Key())
			if !ok {
				it.err = fmt.Errorf("malformed metadata event key %x", it.iter.Key())

				return false
			}

			if !bytes.Equal(g, group) {
				break
			}

			// Events are seq-ascending within the group, so this keeps the
			// verdict of the LATEST event at or below the pin.
			if seq <= it.pin {
				live = op == MetadataEventAdd
			}

			if !it.iter.Next() {
				break
			}
		}

		if live {
			if it.emitOffset > len(group) {
				it.err = fmt.Errorf("malformed metadata event group %x: shorter than its encoded value", group)

				return false
			}

			it.current = group[it.emitOffset:]

			return true
		}
	}

	it.exhausted = true

	return false
}

func (it *EventResolveIterator) Next() bool {
	if it.err != nil || it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if !it.iter.First() {
			it.exhausted = true

			return false
		}
	}

	// After a successful settle the raw iterator already rests on the next
	// group's first key.
	return it.settleFrom(nil)
}

func (it *EventResolveIterator) Current() []byte { return it.current }

func (it *EventResolveIterator) SeekGE(target []byte) bool {
	if it.rangeMode {
		it.err = errInvariantEventRangeSeek

		return false
	}

	if it.err != nil {
		return false
	}

	// A prior failed seek at or below target proves this one empty too.
	if it.floor.covers(target) {
		it.exhausted = true

		return false
	}

	// Absolute reposition: clear the exhausted latch so a re-seek after
	// exhaustion still finds groups (the body re-seeks from target).
	it.exhausted = false
	it.started = true

	seekKey := make([]byte, 0, len(it.seekPrefix)+len(target))
	seekKey = append(seekKey, it.seekPrefix...)
	seekKey = append(seekKey, target...)

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true
		it.floor.fail(target, it.iter.Error())

		return false
	}

	return it.settleFrom(target)
}

func (it *EventResolveIterator) Err() error {
	if it.err != nil {
		return it.err
	}

	return it.iter.Error()
}

func (it *EventResolveIterator) Close() {
	_ = it.iter.Close()
}
