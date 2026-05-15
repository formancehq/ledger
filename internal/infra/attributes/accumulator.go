package attributes

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// ComputedEntry holds a computed attribute value alongside its canonical key.
type ComputedEntry[V proto.Message] struct {
	CanonicalKey []byte
	Value        V
}

// accumulatorBase holds the shared computation state used by both
// Accumulator (slice-based) and streamingAccumulator (callback-based).
// It tracks the latest value per canonical key during a forward scan.
type accumulatorBase[V proto.Message] struct {
	attr             *Attribute[V]
	currentCanonical string
	baseValue        V
}

// feed processes a raw Pebble key-value pair, updating state.
// When a canonical key boundary is crossed, it returns the result for the
// previous key as prev (non-nil). The caller must handle prev
// before the next call.
func (ab *accumulatorBase[V]) feed(pebbleKey, pebbleValue []byte) (matched bool, prev *ComputedEntry[V], err error) {
	if len(pebbleKey) <= 1+AttrTypeLen {
		return false, nil, nil
	}

	attrType := pebbleKey[1]
	if attrType != ab.attr.prefix {
		return false, nil, nil
	}

	canonical := string(pebbleKey[2:])

	if canonical != ab.currentCanonical {
		// Return the previous canonical key's result before resetting.
		if ab.currentCanonical != "" {
			if (any)(ab.baseValue) != nil {
				prev = &ComputedEntry[V]{
					CanonicalKey: []byte(ab.currentCanonical),
					Value:        ab.baseValue,
				}
			}
		}

		ab.currentCanonical = canonical

		var zero V

		ab.baseValue = zero
	}

	v := ab.attr.newValue()
	if err := unmarshalProto(pebbleValue, v); err != nil {
		return false, nil, fmt.Errorf("unmarshaling value: %w", err)
	}

	// Latest entry wins (entries are in Pebble key order = raft index order)
	ab.baseValue = v

	return true, prev, nil
}

// flush computes and returns the entry for the current canonical key (if any),
// then resets the accumulator state.
func (ab *accumulatorBase[V]) flush() *ComputedEntry[V] {
	if ab.currentCanonical == "" {
		return nil
	}

	value := ab.baseValue
	key := ab.currentCanonical
	ab.currentCanonical = ""

	var zero V

	ab.baseValue = zero

	if (any)(value) != nil {
		return &ComputedEntry[V]{
			CanonicalKey: []byte(key),
			Value:        value,
		}
	}

	return nil
}

// Accumulator collects attribute entries fed in Pebble key order and computes
// final values per unique canonical key. It tracks state and flushes
// the computed value when a canonical key boundary is crossed.
//
// Usage: create via NewAccumulator, call Feed for each Pebble key-value pair,
// call Flush when a logical group boundary is reached (e.g., a different entity).
type Accumulator[V proto.Message] struct {
	accumulatorBase[V]

	pending []ComputedEntry[V]
}

// NewAccumulator creates an Accumulator for this attribute type.
func (a *Attribute[V]) NewAccumulator() *Accumulator[V] {
	return &Accumulator[V]{accumulatorBase: accumulatorBase[V]{attr: a}}
}

// Prefix returns the attribute type prefix byte.
func (acc *Accumulator[V]) Prefix() byte {
	return acc.attr.prefix
}

// Feed processes a raw Pebble key-value pair from the attribute range.
// Returns true if the entry matched this accumulator's attribute type and was consumed.
// Entries must be fed in Pebble key order for correct computation.
func (acc *Accumulator[V]) Feed(pebbleKey, pebbleValue []byte) (bool, error) {
	matched, prev, err := acc.feed(pebbleKey, pebbleValue)
	if err != nil {
		return false, err
	}

	if !matched {
		return false, nil
	}

	if prev != nil {
		acc.pending = append(acc.pending, *prev)
	}

	return true, nil
}

// Flush computes any pending value and returns all accumulated results.
// Resets the accumulator for the next group.
func (acc *Accumulator[V]) Flush() []ComputedEntry[V] {
	if entry := acc.flush(); entry != nil {
		acc.pending = append(acc.pending, *entry)
	}

	results := acc.pending
	acc.pending = nil

	return results
}
