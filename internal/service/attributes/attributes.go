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
// - Each Attribute instance has its own KeyBuilder.
// - Use dependency injection (New) to get separate instances per Raft node.
// - Read methods (ComputeValue, List) create their own KeyBuilder for concurrent access.
type Attribute[V proto.Message] struct {
	prefix      byte
	newValue    func() V
	computeFn   func(base V, lastDiff V) V
	kb          *data.KeyBuilder // Used by write methods - each instance has its own
	protoBuffer []byte
}

// SetBase stores a base value for the given canonical key at the specified raft index.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: Uses the instance's KeyBuilder - ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) SetBase(batch *data.Batch, index uint64, canonicalKey []byte, base V) error {
	key := a.kb.
		PutByte(data.KeyPrefixAttributes).
		PutByte(a.prefix).
		PutBytes(canonicalKey).
		PutUInt64(index).
		PutByte(0).
		Build()

	valueBytes, err := proto.MarshalOptions{}.MarshalAppend(a.protoBuffer[:0], base)
	if err != nil {
		return fmt.Errorf("marshaling base value: %w", err)
	}
	a.protoBuffer = valueBytes

	return batch.Set(key, valueBytes, pebble.NoSync)
}

// AddDiff stores a diff value for the given canonical key at the specified raft index.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: Uses the instance's KeyBuilder - ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) AddDiff(batch *data.Batch, index uint64, canonicalKey []byte, diff V) error {
	key := a.kb.
		PutByte(data.KeyPrefixAttributes).
		PutByte(a.prefix).
		PutBytes(canonicalKey).
		PutUInt64(index).
		PutByte(1).
		Build()

	valueBytes, err := proto.MarshalOptions{}.MarshalAppend(a.protoBuffer[:0], diff)
	if err != nil {
		return fmt.Errorf("marshaling diff value: %w", err)
	}
	a.protoBuffer = valueBytes

	return batch.Set(key, valueBytes, pebble.NoSync)
}

// ComputeValue computes the final value for the given canonical key at the specified raft index.
// It finds the most recent base with index <= maxIndex and applies all diffs with index <= maxIndex.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: This is a read operation that can be called concurrently, so it creates its own KeyBuilder.
func (a *Attribute[V]) ComputeValue(s *data.Store, index uint64, canonicalKey []byte) (V, error) {
	var zeroValue V

	// Create a local KeyBuilder for thread-safe concurrent access
	kb := data.NewKeyBuilder()

	// Build the prefix for this attribute type + canonical key
	kb.PutByte(data.KeyPrefixAttributes).
		PutByte(a.prefix).
		PutBytes(canonicalKey)
	lowerBound := kb.Snapshot()

	// Upper bound includes entries at index. Use index+1 unless it would overflow.
	// In case of overflow (index is max uint64), add 0xFF byte to reach past all entries.
	if index == ^uint64(0) {
		kb.PutByte(0xFF)
	} else {
		kb.PutUInt64(index + 1)
	}
	upperBound := kb.Build()

	iter, err := s.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
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
// Note: Uses the instance's KeyBuilder - ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) Delete(batch *data.Batch, canonicalKey []byte) error {
	a.kb.PutByte(data.KeyPrefixAttributes).
		PutByte(a.prefix).
		PutBytes(canonicalKey)
	lowerBound := a.kb.Snapshot()

	// Upper bound: past all possible entries for this canonical key.
	// Key structure: [prefix][attr_prefix][canonicalKey][index(8 bytes)][type(1 byte)]
	// Using max index + 0xFF ensures we're past any valid entry type (0 or 1).
	a.kb.PutUInt64(^uint64(0)).PutByte(0xFF)
	upperBound := a.kb.Build()

	return batch.DeleteRange(lowerBound, upperBound, pebble.NoSync)
}

// DeleteOldest deletes all entries (bases and diffs) with raft index strictly less than the given index.
// This is used to clean up old data after consolidating into a new base.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: Uses the instance's KeyBuilder - ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) DeleteOldest(batch *data.Batch, index uint64, canonicalKey []byte) error {
	// Build lower bound: [keyPrefixAttributes][a.prefix][canonicalKey]
	a.kb.PutByte(data.KeyPrefixAttributes).
		PutByte(a.prefix).
		PutBytes(canonicalKey)
	lowerBound := a.kb.Snapshot()

	// Build upper bound: [keyPrefixAttributes][a.prefix][canonicalKey][index]
	// This is exclusive, so entries at `index` are kept
	a.kb.PutUInt64(index)
	upperBound := a.kb.Build()

	return batch.DeleteRange(lowerBound, upperBound, pebble.NoSync)
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
// Thread-safe: creates its own KeyBuilder for concurrent access.
func (a *Attribute[V]) ScanEntries(s *data.Store, canonicalKey []byte) (*ScanResult[V], error) {
	kb := data.NewKeyBuilder()

	kb.PutByte(data.KeyPrefixAttributes).
		PutByte(a.prefix).
		PutBytes(canonicalKey)
	lowerBound := kb.Snapshot()

	kb.PutByte(0xFF)
	upperBound := kb.Build()

	iter, err := s.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
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
// Note: List creates its own KeyBuilder as it may be called concurrently.
func (a *Attribute[V]) List(s *data.Store) ([]ListEntry, error) {
	kb := data.NewKeyBuilder()

	// Iterate over actual attribute data (not mapping)
	kb.PutByte(data.KeyPrefixAttributes).PutByte(a.prefix)
	lowerBound := kb.Snapshot()

	// Upper bound: same prefix + 0xFF to get all entries
	kb.PutByte(0xFF)
	upperBound := kb.Build()

	iter, err := s.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
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
