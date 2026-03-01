package attributes

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"google.golang.org/protobuf/proto"
)

// ComputedEntry holds a computed attribute value alongside its canonical key.
type ComputedEntry[V proto.Message] struct {
	CanonicalKey []byte
	Value        V
}

// accumulatorBase holds the shared base/diff computation state used by both
// Accumulator (slice-based) and streamingAccumulator (callback-based).
type accumulatorBase[V proto.Message] struct {
	attr             *core[V]
	currentCanonical string
	baseValue        V
	baseIndex        uint64
	lastDiff         V
}

// feed processes a raw Pebble key-value pair, updating base/diff state.
// When a canonical key boundary is crossed, it computes the result for the
// previous key and returns it as prev (non-nil). The caller must handle prev
// before the next call.
func (ab *accumulatorBase[V]) feed(pebbleKey, pebbleValue []byte) (matched bool, prev *ComputedEntry[V], err error) {
	if len(pebbleKey) <= 1+SuffixLen {
		return false, nil, nil
	}

	attrType := pebbleKey[len(pebbleKey)-SuffixLen]
	if attrType != ab.attr.prefix {
		return false, nil, nil
	}

	canonical := string(pebbleKey[1 : len(pebbleKey)-SuffixLen])
	raftIndex := binary.BigEndian.Uint64(pebbleKey[len(pebbleKey)-9 : len(pebbleKey)-1])
	entryType := pebbleKey[len(pebbleKey)-1]

	if canonical != ab.currentCanonical {
		// Compute the previous canonical key's result before resetting.
		if ab.currentCanonical != "" {
			computed := ab.attr.resolveFn(ab.baseValue, ab.lastDiff)
			if (any)(computed) != nil {
				prev = &ComputedEntry[V]{
					CanonicalKey: []byte(ab.currentCanonical),
					Value:        computed,
				}
			}
		}
		ab.currentCanonical = canonical
		var zero V
		ab.baseValue = zero
		ab.baseIndex = 0
		ab.lastDiff = zero
	}

	v := ab.attr.newValue()
	if err := unmarshalProto(pebbleValue, v); err != nil {
		return false, nil, fmt.Errorf("unmarshaling value: %w", err)
	}

	switch entryType {
	case 0: // base
		ab.baseValue = v
		ab.baseIndex = raftIndex
		var zero V
		ab.lastDiff = zero
	case 1: // diff
		if (any)(ab.baseValue) == nil || raftIndex > ab.baseIndex {
			ab.lastDiff = v
		}
	}

	return true, prev, nil
}

// flush computes and returns the entry for the current canonical key (if any),
// then resets the accumulator state.
func (ab *accumulatorBase[V]) flush() *ComputedEntry[V] {
	if ab.currentCanonical == "" {
		return nil
	}
	computed := ab.attr.resolveFn(ab.baseValue, ab.lastDiff)
	key := ab.currentCanonical
	ab.currentCanonical = ""
	var zero V
	ab.baseValue = zero
	ab.lastDiff = zero
	ab.baseIndex = 0
	if (any)(computed) != nil {
		return &ComputedEntry[V]{
			CanonicalKey: []byte(key),
			Value:        computed,
		}
	}
	return nil
}

// Accumulator collects attribute entries fed in Pebble key order and computes
// final values per unique canonical key. It tracks base/diff state and flushes
// the computed value when a canonical key boundary is crossed.
//
// Usage: create via NewAccumulator, call Feed for each Pebble key-value pair,
// call Flush when a logical group boundary is reached (e.g., a different entity).
type Accumulator[V proto.Message] struct {
	accumulatorBase[V]
	pending []ComputedEntry[V]
}

// NewAccumulator creates an Accumulator for this attribute type.
func (a *core[V]) NewAccumulator() *Accumulator[V] {
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

// AccumulatingAttribute is an attribute type that supports base+diff accumulation.
// It exposes SetBase() and AddDiff() for writing. Used for Volume attributes where
// diffs are cumulative deltas that get added to the base value.
type AccumulatingAttribute[V proto.Message] struct {
	core[V]
}

// SetBase stores a base value for the given canonical key at the specified raft index.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own instance.
func (a *AccumulatingAttribute[V]) SetBase(batch *dal.Batch, index uint64, canonicalKey []byte, base V) error {
	return a.setBase(batch, index, canonicalKey, base)
}

// AddDiff stores a diff value for the given canonical key at the specified raft index.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own instance.
func (a *AccumulatingAttribute[V]) AddDiff(batch *dal.Batch, index uint64, canonicalKey []byte, diff V) error {
	return a.addDiff(batch, index, canonicalKey, diff)
}
