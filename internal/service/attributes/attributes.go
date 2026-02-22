package attributes

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"google.golang.org/protobuf/proto"
)

// Attribute represents a generic attribute type that can be stored with base values and diffs.
// It supports computing the final value by applying diffs to a base value.
// Value is the protobuf message type for the attribute value.
//
// Key layout: [KeyPrefixAttributes (1B)][CanonicalKey (NB)][AttrType (1B)][RaftIndex (8B)][EntryType (1B)]
// The suffix is always 10 bytes: [AttrType 1B][RaftIndex 8B][EntryType 1B].
// This layout co-locates all attributes for the same canonical key in Pebble,
// improving write locality and compaction.
//
// Thread-safety:
// - Each Attribute instance has its own pre-allocated key buffer.
// - Use dependency injection (New) to get separate instances per Raft node.
// - Read methods (ComputeValue, List, ScanEntries) allocate their own buffer for concurrent access.
type Attribute[V proto.Message] struct {
	prefix      byte
	newValue    func() V
	computeFn   func(base V, lastDiff V) V
	keyBuf      []byte // pre-allocated buffer for write-path key construction (reused across calls)
	protoBuffer []byte
}

// ensureKeyBuf ensures keyBuf can hold at least n bytes.
func (a *Attribute[V]) ensureKeyBuf(n int) {
	if len(a.keyBuf) < n {
		a.keyBuf = make([]byte, n)
	}
}

// putPrefix writes [KeyPrefixAttributes][canonicalKey][a.prefix] into buf.
// buf must have at least 2+len(canonicalKey) bytes.
func (a *Attribute[V]) putPrefix(buf []byte, canonicalKey []byte) {
	buf[0] = dal.KeyPrefixAttributes
	copy(buf[1:], canonicalKey)
	buf[1+len(canonicalKey)] = a.prefix
}

// prefixLen returns the number of bytes for [KeyPrefixAttributes][canonicalKey][attrType].
func prefixLen(canonicalKey []byte) int {
	return 2 + len(canonicalKey) // 1 for KeyPrefixAttributes + N for canonicalKey + 1 for attrType
}

// vtSizedMarshaler is implemented by vtprotobuf-generated messages.
type vtSizedMarshaler interface {
	SizeVT() int
	MarshalToVT([]byte) (int, error)
}

// vtUnmarshaler is implemented by vtprotobuf-generated messages.
type vtUnmarshaler interface {
	UnmarshalVT([]byte) error
}

// marshalProto marshals a proto message using vtprotobuf when available,
// falling back to standard proto.MarshalOptions otherwise.
// The provided buf is reused when large enough; the returned slice may be a
// different backing array.
func marshalProto(buf []byte, msg proto.Message) ([]byte, error) {
	if m, ok := msg.(vtSizedMarshaler); ok {
		size := m.SizeVT()
		if cap(buf) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
		n, err := m.MarshalToVT(buf)
		return buf[:n], err
	}
	return proto.MarshalOptions{}.MarshalAppend(buf[:0], msg)
}

// unmarshalProto unmarshals data into a proto message using vtprotobuf when
// available, falling back to standard proto.Unmarshal otherwise.
func unmarshalProto(data []byte, msg proto.Message) error {
	if m, ok := msg.(vtUnmarshaler); ok {
		return m.UnmarshalVT(data)
	}
	return proto.Unmarshal(data, msg)
}

// writeEntry writes a base (entryType=0) or diff (entryType=1) entry to the batch.
// Key format: [KeyPrefixAttributes][canonicalKey][prefix][index BE 8 bytes][entryType].
// Uses the pre-allocated keyBuf — not safe for concurrent use.
func (a *Attribute[V]) writeEntry(batch *dal.Batch, index uint64, canonicalKey []byte, entryType byte, value V) error {
	pLen := prefixLen(canonicalKey)
	keyLen := pLen + 9
	a.ensureKeyBuf(keyLen)
	a.putPrefix(a.keyBuf, canonicalKey)
	binary.BigEndian.PutUint64(a.keyBuf[pLen:], index)
	a.keyBuf[keyLen-1] = entryType

	valueBytes, err := marshalProto(a.protoBuffer, value)
	if err != nil {
		return fmt.Errorf("marshaling value: %w", err)
	}
	a.protoBuffer = valueBytes

	return batch.Set(a.keyBuf[:keyLen], valueBytes, pebble.NoSync)
}

// SetBase stores a base value for the given canonical key at the specified raft index.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) SetBase(batch *dal.Batch, index uint64, canonicalKey []byte, base V) error {
	return a.writeEntry(batch, index, canonicalKey, 0, base)
}

// AddDiff stores a diff value for the given canonical key at the specified raft index.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) AddDiff(batch *dal.Batch, index uint64, canonicalKey []byte, diff V) error {
	return a.writeEntry(batch, index, canonicalKey, 1, diff)
}

// SuffixLen is the fixed suffix length of an attribute Pebble key:
// [AttrType(1)][RaftIndex(8)][EntryType(1)] = 10 bytes.
const SuffixLen = 10

// ComputeValue computes the final value for the given canonical key at the specified raft index.
// It finds the most recent base with index <= maxIndex and applies all diffs with index <= maxIndex.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: This is a read operation — allocates its own buffer for concurrent safety.
func (a *Attribute[V]) ComputeValue(reader dal.PebbleReader, index uint64, canonicalKey []byte) (V, error) {
	var zeroValue V

	// Key prefix: [KeyPrefixAttributes][canonicalKey][attrType]
	pLen := prefixLen(canonicalKey)
	var upperExtra int
	if index == ^uint64(0) {
		upperExtra = 1 // 0xFF sentinel
	} else {
		upperExtra = 8 // index+1 as big-endian uint64
	}
	buf := make([]byte, pLen+upperExtra)
	a.putPrefix(buf, canonicalKey)

	if index == ^uint64(0) {
		buf[pLen] = 0xFF
	} else {
		binary.BigEndian.PutUint64(buf[pLen:], index+1)
	}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: buf[:pLen],
		UpperBound: buf[:pLen+upperExtra],
	})
	if err != nil {
		return zeroValue, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	// Track the most recent base and the last diff after it
	var (
		baseValue V
		baseIndex uint64
		lastDiff  V
	)

	for iter.First(); iter.Valid(); iter.Next() {
		iterKey := iter.Key()

		raftIndex := binary.BigEndian.Uint64(iterKey[len(iterKey)-9 : len(iterKey)-1])
		entryType := iterKey[len(iterKey)-1]

		valueBytes, err := iter.ValueAndErr()
		if err != nil {
			return zeroValue, fmt.Errorf("reading value: %w", err)
		}

		v := a.newValue()
		if err := unmarshalProto(valueBytes, v); err != nil {
			return zeroValue, fmt.Errorf("unmarshaling value: %w", err)
		}

		switch entryType {
		case 0:
			// Base entry - reset computation from this point
			baseValue = v
			baseIndex = raftIndex
			lastDiff = zeroValue
		case 1:
			if (any)(baseValue) == nil || raftIndex > baseIndex {
				lastDiff = v
			}
		}
	}

	return a.computeFn(baseValue, lastDiff), nil
}

// Delete removes all entries (bases and diffs) for the given canonical key at any raft index.
// This performs a physical deletion, removing all historical data for this key.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) Delete(batch *dal.Batch, canonicalKey []byte) error {
	pLen := prefixLen(canonicalKey)
	upperLen := pLen + 9 // +8 for ^uint64(0) + 1 for 0xFF
	a.ensureKeyBuf(upperLen)
	a.putPrefix(a.keyBuf, canonicalKey)
	binary.BigEndian.PutUint64(a.keyBuf[pLen:], ^uint64(0))
	a.keyBuf[pLen+8] = 0xFF

	// Sub-slices of the same buffer are safe — Pebble copies both in DeleteRange.
	return batch.DeleteRange(a.keyBuf[:pLen], a.keyBuf[:upperLen], pebble.NoSync)
}

// DeleteOldest deletes all entries (bases and diffs) with raft index strictly less than the given index.
// This is used to clean up old data after consolidating into a new base.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) DeleteOldest(batch *dal.Batch, index uint64, canonicalKey []byte) error {
	pLen := prefixLen(canonicalKey)
	upperLen := pLen + 8
	a.ensureKeyBuf(upperLen)
	a.putPrefix(a.keyBuf, canonicalKey)
	binary.BigEndian.PutUint64(a.keyBuf[pLen:], index)

	// Sub-slices of the same buffer are safe — Pebble copies both in DeleteRange.
	return batch.DeleteRange(a.keyBuf[:pLen], a.keyBuf[:upperLen], pebble.NoSync)
}

// ScanResult holds the results of scanning all entries for a canonical key.
type ScanResult[V proto.Message] struct {
	LatestBase      V
	LatestBaseIndex uint64
	HasBase         bool
	LatestDiffIndex uint64
	HasDiff         bool
	TotalEntries    int
}

// ScanEntries scans all entries for a canonical key and returns the latest base/diff info.
// Thread-safe: allocates its own buffer for concurrent access.
func (a *Attribute[V]) ScanEntries(reader dal.PebbleReader, canonicalKey []byte) (*ScanResult[V], error) {
	// Single allocation for both bounds.
	pLen := prefixLen(canonicalKey)
	buf := make([]byte, pLen+1)
	a.putPrefix(buf, canonicalKey)
	buf[pLen] = 0xFF

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: buf[:pLen],
		UpperBound: buf[:pLen+1],
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	result := &ScanResult[V]{}

	for iter.First(); iter.Valid(); iter.Next() {
		iterKey := iter.Key()
		result.TotalEntries++

		raftIndex := binary.BigEndian.Uint64(iterKey[len(iterKey)-9 : len(iterKey)-1])
		entryType := iterKey[len(iterKey)-1]

		switch entryType {
		case 0: // base
			if !result.HasBase || raftIndex > result.LatestBaseIndex {
				valueBytes, err := iter.ValueAndErr()
				if err != nil {
					return nil, fmt.Errorf("reading base value: %w", err)
				}
				v := a.newValue()
				if err := unmarshalProto(valueBytes, v); err != nil {
					return nil, fmt.Errorf("unmarshaling base value: %w", err)
				}
				result.LatestBase = v
				result.LatestBaseIndex = raftIndex
				result.HasBase = true
			}
		case 1: // diff
			if !result.HasDiff || raftIndex > result.LatestDiffIndex {
				result.LatestDiffIndex = raftIndex
				result.HasDiff = true
			}
		}
	}

	return result, nil
}

// ComputedEntry holds a computed attribute value alongside its canonical key.
type ComputedEntry[V proto.Message] struct {
	CanonicalKey []byte
	Value        V
}

// accumulatorBase holds the shared base/diff computation state used by both
// Accumulator (slice-based) and streamingAccumulator (callback-based).
type accumulatorBase[V proto.Message] struct {
	attr             *Attribute[V]
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
			computed := ab.attr.computeFn(ab.baseValue, ab.lastDiff)
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
	computed := ab.attr.computeFn(ab.baseValue, ab.lastDiff)
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

// ForEachInPrefix streams computed entries for all canonical keys sharing the
// given prefix. Instead of accumulating results in memory, it calls fn for each
// computed entry at canonical key boundaries. This is O(1) memory (excluding
// the callback's own allocations) vs O(N) for ComputeAllForPrefix.
// Thread-safe: allocates its own buffer for concurrent access.
func (a *Attribute[V]) ForEachInPrefix(
	reader dal.PebbleReader,
	maxIndex uint64,
	canonicalPrefix []byte,
	fn func(entry ComputedEntry[V]) error,
) error {
	lowerBound := make([]byte, 1+len(canonicalPrefix))
	lowerBound[0] = dal.KeyPrefixAttributes
	copy(lowerBound[1:], canonicalPrefix)

	var upperBound []byte
	if incPrefix := IncrementBytes(canonicalPrefix); incPrefix != nil {
		upperBound = make([]byte, 1+len(incPrefix))
		upperBound[0] = dal.KeyPrefixAttributes
		copy(upperBound[1:], incPrefix)
	} else {
		upperBound = []byte{dal.KeyPrefixAttributes + 1}
	}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return fmt.Errorf("creating iterator for prefix scan: %w", err)
	}
	defer func() { _ = iter.Close() }()

	ab := accumulatorBase[V]{attr: a}
	minKeyLen := 1 + SuffixLen

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) <= minKeyLen {
			continue
		}

		raftIndex := binary.BigEndian.Uint64(key[len(key)-9 : len(key)-1])
		if raftIndex > maxIndex {
			continue
		}

		valueBytes, err := iter.ValueAndErr()
		if err != nil {
			return fmt.Errorf("reading value: %w", err)
		}

		_, prev, err := ab.feed(key, valueBytes)
		if err != nil {
			return err
		}
		if prev != nil {
			if err := fn(*prev); err != nil {
				return err
			}
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}

	// Flush the last canonical key.
	if entry := ab.flush(); entry != nil {
		if err := fn(*entry); err != nil {
			return err
		}
	}

	return nil
}

// ComputeAllForPrefix computes the final value for all canonical keys sharing the
// given prefix. It performs a single forward scan using ForEachInPrefix internally.
// This is more efficient than List + ComputeValue per key, as it uses one iterator
// scoped to just the prefix range instead of the entire attribute space.
// Thread-safe: allocates its own buffer for concurrent access.
func (a *Attribute[V]) ComputeAllForPrefix(reader dal.PebbleReader, maxIndex uint64, canonicalPrefix []byte) ([]ComputedEntry[V], error) {
	var results []ComputedEntry[V]
	err := a.ForEachInPrefix(reader, maxIndex, canonicalPrefix, func(entry ComputedEntry[V]) error {
		results = append(results, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// AttrTypeFromKey extracts the attribute type prefix byte from a Pebble attribute key.
// Returns (attrType, true) on success, or (0, false) if the key is too short.
func AttrTypeFromKey(pebbleKey []byte) (byte, bool) {
	if len(pebbleKey) <= 1+SuffixLen {
		return 0, false
	}
	return pebbleKey[len(pebbleKey)-SuffixLen], true
}

// CanonicalKeyFromPebbleKey extracts the canonical key from a Pebble attribute key.
// Returns nil if the key is too short.
func CanonicalKeyFromPebbleKey(pebbleKey []byte) []byte {
	if len(pebbleKey) <= 1+SuffixLen {
		return nil
	}
	return pebbleKey[1 : len(pebbleKey)-SuffixLen]
}

// IncrementBytes increments a byte slice by 1 (treating as big-endian unsigned integer).
// Returns nil if all bytes are 0xFF (overflow).
func IncrementBytes(b []byte) []byte {
	result := make([]byte, len(b))
	copy(result, b)
	for i := len(result) - 1; i >= 0; i-- {
		result[i]++
		if result[i] != 0 {
			return result
		}
	}
	return nil
}

// compactKey compacts a single canonical key: computes the final value, deletes all entries,
// and writes a single base entry at targetIndex.
func (a *Attribute[V]) compactKey(s *dal.Store, batch *dal.Batch, targetIndex uint64, canonicalKey []byte) error {
	value, err := a.ComputeValue(s, ^uint64(0), canonicalKey)
	if err != nil {
		return fmt.Errorf("computing value for key %x: %w", canonicalKey, err)
	}

	if err := a.Delete(batch, canonicalKey); err != nil {
		return fmt.Errorf("deleting entries for key %x: %w", canonicalKey, err)
	}

	if (any)(value) != nil && !proto.Equal(value, a.newValue()) {
		if err := a.SetBase(batch, targetIndex, canonicalKey, value); err != nil {
			return fmt.Errorf("setting base for key %x: %w", canonicalKey, err)
		}
	}

	return nil
}
