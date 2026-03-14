package attributes

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// Attribute is the implementation for all attribute types.
// It holds the Pebble key layout, serialization helpers, and entry write logic.
//
// Key layout: [KeyPrefixAttributes (1B)][CanonicalKey (NB)][AttrType (1B)][RaftIndex (8B)]
// The suffix is always 9 bytes: [AttrType 1B][RaftIndex 8B].
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

// Set stores a value for the given canonical key at the specified raft index.
// Key format: [KeyPrefixAttributes][canonicalKey][prefix][index BE 8 bytes].
// Uses the pre-allocated keyBuf — not safe for concurrent use.
func (a *Attribute[V]) Set(batch *dal.Batch, index uint64, canonicalKey []byte, value V) error {
	pLen := prefixLen(canonicalKey)
	keyLen := pLen + 8
	a.ensureKeyBuf(keyLen)
	a.putPrefix(a.keyBuf, canonicalKey)
	binary.BigEndian.PutUint64(a.keyBuf[pLen:], index)

	valueBytes, err := marshalProto(a.protoBuffer, value)
	if err != nil {
		return fmt.Errorf("marshaling value: %w", err)
	}

	a.protoBuffer = valueBytes

	return batch.Set(a.keyBuf[:keyLen], valueBytes, pebble.NoSync)
}

// SuffixLen is the fixed suffix length of an attribute Pebble key:
// [AttrType(1)][RaftIndex(8)] = 9 bytes.
const SuffixLen = 9

// ComputeValue computes the final value for the given canonical key at the specified raft index.
// It finds the most recent entry with index <= maxIndex.
// Returns the value, the raft index of the latest Pebble entry (0 if no entry found),
// and any error. The returned index enables point deletes instead of range tombstones
// when the entry is later overwritten.
// Note: This is a read operation — allocates its own buffer for concurrent safety.
func (a *Attribute[V]) ComputeValue(reader dal.PebbleReader, index uint64, canonicalKey []byte) (V, uint64, error) {
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
		return zeroValue, 0, fmt.Errorf("creating iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	// Track the most recent value and its raft index
	var latestValue V
	var latestIndex uint64

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		latestIndex = binary.BigEndian.Uint64(key[len(key)-8:])

		valueBytes, err := iter.ValueAndErr()
		if err != nil {
			return zeroValue, 0, fmt.Errorf("reading value: %w", err)
		}

		v := a.newValue()
		if err := unmarshalProto(valueBytes, v); err != nil {
			return zeroValue, 0, fmt.Errorf("unmarshaling value: %w", err)
		}

		latestValue = v
	}

	return latestValue, latestIndex, nil
}

// Delete removes all entries for the given canonical key at any raft index.
// This performs a physical deletion, removing all historical data for this key.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own instance.
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

// DeleteAt deletes the entry at a specific raft index for the given canonical key.
// This performs a point delete (no range tombstone), which is more efficient than DeleteOldest
// when the exact previous index is known.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own instance.
func (a *Attribute[V]) DeleteAt(batch *dal.Batch, index uint64, canonicalKey []byte) error {
	pLen := prefixLen(canonicalKey)
	keyLen := pLen + 8
	a.ensureKeyBuf(keyLen)
	a.putPrefix(a.keyBuf, canonicalKey)
	binary.BigEndian.PutUint64(a.keyBuf[pLen:], index)

	return batch.DeleteKey(a.keyBuf[:keyLen])
}

// DeleteOldest deletes all entries with raft index strictly less than the given index.
// This is used to clean up old data after consolidating into a new base.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own instance.
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
	TotalEntries    int
}

// ScanEntries scans all entries for a canonical key and returns the latest entry info.
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

		raftIndex := binary.BigEndian.Uint64(iterKey[len(iterKey)-8:])

		if !result.HasBase || raftIndex > result.LatestBaseIndex {
			valueBytes, err := iter.ValueAndErr()
			if err != nil {
				return nil, fmt.Errorf("reading value: %w", err)
			}

			v := a.newValue()
			if err := unmarshalProto(valueBytes, v); err != nil {
				return nil, fmt.Errorf("unmarshaling value: %w", err)
			}

			result.LatestBase = v
			result.LatestBaseIndex = raftIndex
			result.HasBase = true
		}
	}

	return result, nil
}

// ComputeAllForPrefix computes the final value for all canonical keys sharing the
// given prefix. It performs a single forward scan using NewStreamingIter internally.
// This is more efficient than List + ComputeValue per key, as it uses one iterator
// scoped to just the prefix range instead of the entire attribute space.
// Thread-safe: allocates its own buffer for concurrent access.
func (a *Attribute[V]) ComputeAllForPrefix(reader dal.PebbleReader, canonicalPrefix []byte) ([]ComputedEntry[V], error) {
	si, err := a.NewStreamingIter(reader, canonicalPrefix)
	if err != nil {
		return nil, err
	}

	defer func() { _ = si.Close() }()

	var results []ComputedEntry[V]

	for si.Next() {
		results = append(results, si.Entry())
	}

	if err := si.Err(); err != nil {
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
