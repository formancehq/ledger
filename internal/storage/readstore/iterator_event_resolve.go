package readstore

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// EventResolveIterator reads one (metadata key, encoded value) event range at
// a pinned raft sequence, yielding — in entity order — every entity whose
// latest event at or below the pin is an ADD (design sketch, see
// event_keys.go).
//
// Events of one (value, entity) group are key-adjacent and seq-ascending, so
// resolution is a single forward pass: within a group, the last event with
// seq <= pin decides; events above the pin are invisible to this reader by
// construction. Dead groups (DEL or nothing at the pin) are skipped whole.
//
// The absolute SeekGE contract (iterator-seek-contract.md) holds: a seek
// positions at the first event key of the first group whose entity >= target
// and resolves forward. The seekFloor exhaustion cache is deliberately
// omitted from the sketch.
type EventResolveIterator struct {
	iter   *pebble.Iterator
	prefix []byte // through the encoded value
	pin    uint64

	current   []byte
	started   bool
	exhausted bool
	err       error
}

// NewEventResolveIterator scans the event range under prefix (built by
// MetadataIndexEventValuePrefixV) as of pin.
func NewEventResolveIterator(reader dal.PebbleReader, prefix []byte, pin uint64) (*EventResolveIterator, error) {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: IncrementBytes(prefix),
	})
	if err != nil {
		return nil, err
	}

	return &EventResolveIterator{iter: iter, prefix: prefix, pin: pin}, nil
}

// parse splits an event key into (entity, seq, op). The terminator position
// is unambiguous from the right because entities cannot contain NUL.
func (it *EventResolveIterator) parse(key []byte) (entity []byte, seq uint64, op byte, ok bool) {
	rest := key[len(it.prefix):]
	tpos := len(rest) - metadataEventSuffixLen - 1
	if tpos < 0 || rest[tpos] != metadataEventTerminator {
		return nil, 0, 0, false
	}

	return rest[:tpos], binary.BigEndian.Uint64(rest[tpos+1 : tpos+9]), rest[tpos+9], true
}

// settle resolves consecutive groups starting at the raw iterator's current
// position until one is live at the pin, leaving the raw iterator at the
// following group.
func (it *EventResolveIterator) settle() bool {
	for it.iter.Valid() {
		entity, _, _, ok := it.parse(it.iter.Key())
		if !ok {
			it.err = fmt.Errorf("malformed metadata event key %x", it.iter.Key())

			return false
		}

		group := append([]byte(nil), entity...)
		live := false

		for it.iter.Valid() {
			e, seq, op, ok := it.parse(it.iter.Key())
			if !ok {
				it.err = fmt.Errorf("malformed metadata event key %x", it.iter.Key())

				return false
			}

			if !bytes.Equal(e, group) {
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
			it.current = group

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
	return it.settle()
}

func (it *EventResolveIterator) Current() []byte { return it.current }

func (it *EventResolveIterator) SeekGE(target []byte) bool {
	if it.err != nil {
		return false
	}

	it.exhausted = false
	it.started = true

	seekKey := make([]byte, 0, len(it.prefix)+len(target))
	seekKey = append(seekKey, it.prefix...)
	seekKey = append(seekKey, target...)

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true

		return false
	}

	return it.settle()
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
