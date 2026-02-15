package attributes

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"google.golang.org/protobuf/proto"
)

// Attribute represents a generic attribute type that can be stored with base values and diffs.
// It supports computing the final value by applying diffs to a base value.
// Value is the protobuf message type for the attribute value.
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

// putPrefix writes [KeyPrefixAttributes][a.prefix][canonicalKey] into buf.
// buf must have at least 2+len(canonicalKey) bytes.
func (a *Attribute[V]) putPrefix(buf []byte, canonicalKey []byte) {
	buf[0] = data.KeyPrefixAttributes
	buf[1] = a.prefix
	copy(buf[2:], canonicalKey)
}

// writeEntry writes a base (entryType=0) or diff (entryType=1) entry to the batch.
// Key format: [KeyPrefixAttributes][prefix][canonicalKey][index BE 8 bytes][entryType].
// Uses the pre-allocated keyBuf — not safe for concurrent use.
func (a *Attribute[V]) writeEntry(batch *data.Batch, index uint64, canonicalKey []byte, entryType byte, value V) error {
	prefixLen := 2 + len(canonicalKey)
	keyLen := prefixLen + 9
	a.ensureKeyBuf(keyLen)
	a.putPrefix(a.keyBuf, canonicalKey)
	binary.BigEndian.PutUint64(a.keyBuf[prefixLen:], index)
	a.keyBuf[keyLen-1] = entryType

	valueBytes, err := proto.MarshalOptions{}.MarshalAppend(a.protoBuffer[:0], value)
	if err != nil {
		return fmt.Errorf("marshaling value: %w", err)
	}
	a.protoBuffer = valueBytes

	return batch.Set(a.keyBuf[:keyLen], valueBytes, pebble.NoSync)
}

// SetBase stores a base value for the given canonical key at the specified raft index.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) SetBase(batch *data.Batch, index uint64, canonicalKey []byte, base V) error {
	return a.writeEntry(batch, index, canonicalKey, 0, base)
}

// AddDiff stores a diff value for the given canonical key at the specified raft index.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) AddDiff(batch *data.Batch, index uint64, canonicalKey []byte, diff V) error {
	return a.writeEntry(batch, index, canonicalKey, 1, diff)
}

// ComputeValue computes the final value for the given canonical key at the specified raft index.
// It finds the most recent base with index <= maxIndex and applies all diffs with index <= maxIndex.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: This is a read operation — allocates its own buffer for concurrent safety.
func (a *Attribute[V]) ComputeValue(s *data.Store, index uint64, canonicalKey []byte) (V, error) {
	var zeroValue V

	// Single allocation for both lower and upper bounds (sub-slices of the same buffer).
	prefixLen := 2 + len(canonicalKey)
	var upperExtra int
	if index == ^uint64(0) {
		upperExtra = 1 // 0xFF sentinel
	} else {
		upperExtra = 8 // index+1 as big-endian uint64
	}
	buf := make([]byte, prefixLen+upperExtra)
	a.putPrefix(buf, canonicalKey)

	if index == ^uint64(0) {
		buf[prefixLen] = 0xFF
	} else {
		binary.BigEndian.PutUint64(buf[prefixLen:], index+1)
	}

	iter, err := s.NewIter(&pebble.IterOptions{
		LowerBound: buf[:prefixLen],
		UpperBound: buf[:prefixLen+upperExtra],
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
		if err := proto.Unmarshal(valueBytes, v); err != nil {
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
func (a *Attribute[V]) Delete(batch *data.Batch, canonicalKey []byte) error {
	prefixLen := 2 + len(canonicalKey)
	upperLen := prefixLen + 9 // +8 for ^uint64(0) + 1 for 0xFF
	a.ensureKeyBuf(upperLen)
	a.putPrefix(a.keyBuf, canonicalKey)
	binary.BigEndian.PutUint64(a.keyBuf[prefixLen:], ^uint64(0))
	a.keyBuf[prefixLen+8] = 0xFF

	// Sub-slices of the same buffer are safe — Pebble copies both in DeleteRange.
	return batch.DeleteRange(a.keyBuf[:prefixLen], a.keyBuf[:upperLen], pebble.NoSync)
}

// DeleteOldest deletes all entries (bases and diffs) with raft index strictly less than the given index.
// This is used to clean up old data after consolidating into a new base.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) DeleteOldest(batch *data.Batch, index uint64, canonicalKey []byte) error {
	prefixLen := 2 + len(canonicalKey)
	upperLen := prefixLen + 8
	a.ensureKeyBuf(upperLen)
	a.putPrefix(a.keyBuf, canonicalKey)
	binary.BigEndian.PutUint64(a.keyBuf[prefixLen:], index)

	// Sub-slices of the same buffer are safe — Pebble copies both in DeleteRange.
	return batch.DeleteRange(a.keyBuf[:prefixLen], a.keyBuf[:upperLen], pebble.NoSync)
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
func (a *Attribute[V]) ScanEntries(s *data.Store, canonicalKey []byte) (*ScanResult[V], error) {
	// Single allocation for both bounds.
	prefixLen := 2 + len(canonicalKey)
	buf := make([]byte, prefixLen+1)
	a.putPrefix(buf, canonicalKey)
	buf[prefixLen] = 0xFF

	iter, err := s.NewIter(&pebble.IterOptions{
		LowerBound: buf[:prefixLen],
		UpperBound: buf[:prefixLen+1],
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
				if err := proto.Unmarshal(valueBytes, v); err != nil {
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

// ListEntry represents an entry found when listing attributes.
// It contains the canonical key bytes extracted from the Pebble key.
type ListEntry struct {
	// CanonicalKey is the original key bytes
	CanonicalKey []byte
}

// List returns all unique canonical keys for this attribute type.
// It iterates over the actual attribute data and extracts unique canonical keys.
// Note: Allocates its own buffer for concurrent safety.
func (a *Attribute[V]) List(s *data.Store) ([]ListEntry, error) {
	// Single allocation for both bounds.
	buf := make([]byte, 3)
	buf[0] = data.KeyPrefixAttributes
	buf[1] = a.prefix
	buf[2] = 0xFF

	iter, err := s.NewIter(&pebble.IterOptions{
		LowerBound: buf[:2],
		UpperBound: buf[:3],
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for attributes: %w", err)
	}
	defer func() { _ = iter.Close() }()

	// Use a map to track unique canonical keys (since each key may have multiple entries at different indexes)
	seen := make(map[string]struct{})
	var entries []ListEntry

	// Key structure: [KeyPrefixAttributes][prefix][canonical_key_bytes][index (8 bytes)][type (1 byte)]
	// We need to extract the canonical key by removing prefix (2 bytes) and suffix (9 bytes)
	prefixLen := 2 // KeyPrefixAttributes + prefix
	suffixLen := 9 // index (8 bytes) + type (1 byte)
	minKeyLen := prefixLen + suffixLen

	for iter.First(); iter.Valid(); iter.Next() {
		iterKey := iter.Key()
		if len(iterKey) <= minKeyLen {
			continue // Skip invalid keys
		}

		// Extract canonical key bytes (between prefix and suffix)
		canonicalKeyLen := len(iterKey) - prefixLen - suffixLen
		canonicalKey := string(iterKey[prefixLen : prefixLen+canonicalKeyLen])

		// Skip if we've already seen this canonical key
		if _, ok := seen[canonicalKey]; ok {
			continue
		}
		seen[canonicalKey] = struct{}{}

		// Make a copy for the entry
		canonicalBytes := make([]byte, canonicalKeyLen)
		copy(canonicalBytes, iterKey[prefixLen:prefixLen+canonicalKeyLen])

		entries = append(entries, ListEntry{
			CanonicalKey: canonicalBytes,
		})
	}

	return entries, nil
}
