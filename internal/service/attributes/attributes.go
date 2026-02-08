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
	computeFn   func(value V, diffs []V) V
	kb          *data.KeyBuilder // Used by write methods - each instance has its own
	protoBuffer []byte
}

// SetBase stores a base value for the given key ID at the specified raft index.
// Note: Uses the instance's KeyBuilder - ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) SetBase(batch *data.Batch, index uint64, id U128, base V) error {
	key := a.kb.
		PutByte(data.KeyPrefixAttributes).
		PutBytes(id.Bytes()).
		PutByte(a.prefix).
		PutUInt64(index).
		PutByte(0).
		Build()

	valueBytes, err := proto.MarshalOptions{}.MarshalAppend(a.protoBuffer, base)
	if err != nil {
		return fmt.Errorf("marshaling base value: %w", err)
	}

	return batch.Set(key, valueBytes, pebble.NoSync)
}

// AddDiff stores a diff value for the given key ID at the specified raft index.
// Note: Uses the instance's KeyBuilder - ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) AddDiff(batch *data.Batch, index uint64, id U128, diff V) error {
	key := a.kb.
		PutByte(data.KeyPrefixAttributes).
		PutBytes(id.Bytes()).
		PutByte(a.prefix).
		PutUInt64(index).
		PutByte(1).
		Build()

	valueBytes, err := proto.MarshalOptions{}.MarshalAppend(a.protoBuffer, diff)
	if err != nil {
		return fmt.Errorf("marshaling diff value: %w", err)
	}

	return batch.Set(key, valueBytes, pebble.NoSync)
}

// ComputeValue computes the final value for the given key ID at the specified raft index.
// It finds the most recent base with index <= maxIndex and applies all diffs with index <= maxIndex.
// Note: This is a read operation that can be called concurrently, so it creates its own KeyBuilder.
func (a *Attribute[V]) ComputeValue(s *data.Store, index uint64, id U128) (V, error) {
	var zeroValue V

	// Create a local KeyBuilder for thread-safe concurrent access
	kb := data.NewKeyBuilder()

	// Build the prefix for this key + attribute prefix
	kb.PutByte(data.KeyPrefixAttributes).
		PutBytes(id.Bytes()).
		PutByte(a.prefix)
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

	// Track the most recent base and diffs after it
	var (
		baseValue V
		baseIndex uint64
		diffs     []V
	)

	for iter.First(); iter.Valid(); iter.Next() {
		iterKey := iter.Key()

		raftIndex := binary.BigEndian.Uint64(iterKey[len(iterKey)-9 : len(iterKey)-1])
		entryType := iterKey[len(iterKey)-1]

		valueBytes, err := iter.ValueAndErr()
		if err != nil {
			return zeroValue, fmt.Errorf("reading value: %w", err)
		}

		// Create a new instance of Value type for unmarshaling
		v := a.newValue()
		if err := proto.Unmarshal(valueBytes, v); err != nil {
			return zeroValue, fmt.Errorf("unmarshaling value: %w", err)
		}

		switch entryType {
		case 0:
			// Base entry - reset computation from this point
			baseValue = v
			baseIndex = raftIndex
			diffs = nil
		case 1:
			if (any)(baseValue) == nil || raftIndex > baseIndex {
				diffs = append(diffs, v)
			}
		}
	}

	return a.computeFn(baseValue, diffs), nil
}

// DeleteOldest deletes all entries (bases and diffs) with raft index strictly less than the given index.
// This is used to clean up old data after consolidating into a new base.
// Note: Uses the instance's KeyBuilder - ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) DeleteOldest(batch *data.Batch, index uint64, id U128) error {
	// Build lower bound: [keyPrefixAttributes][id][a.prefix]
	a.kb.PutByte(data.KeyPrefixAttributes).
		PutBytes(id.Bytes()).
		PutByte(a.prefix)
	lowerBound := a.kb.Snapshot()

	// Build upper bound: [keyPrefixAttributes][id][a.prefix][index]
	// This is exclusive, so entries at `index` are kept
	a.kb.PutUInt64(index)
	upperBound := a.kb.Build()

	return batch.DeleteRange(lowerBound, upperBound, pebble.NoSync)
}

// Touch stores the mapping from canonical key bytes to its hash (U128 + tag64).
// Note: Uses the instance's KeyBuilder - ensure each Raft node has its own Attribute instance.
func (a *Attribute[V]) Touch(batch *data.Batch, canonicalKey []byte, id U128, tag uint64) error {
	value := make([]byte, 24)
	copy(value, id.Bytes())
	binary.BigEndian.PutUint64(value[16:], tag)

	key := a.kb.
		PutByte(data.KeyPrefixAttributesMapping).
		PutByte(a.prefix).
		PutBytes(canonicalKey).
		Build()

	return batch.Set(key, value, pebble.NoSync)
}

// MappingEntry represents an entry in the attributes mapping.
// It contains the hash and the original key bytes.
type MappingEntry struct {
	// Hash128 is the 128-bit hash (U128)
	Hash128 U128
	// Hash64 is the secondary 64-bit tag for collision detection
	Hash64 uint64
	// CanonicalKey is the original key bytes
	CanonicalKey []byte
}

// List returns all mapping entries for this attribute type.
// Each entry contains the hash (U128 + tag64) and the canonical key bytes.
// Note: List creates its own KeyBuilder as it may be called concurrently.
func (a *Attribute[V]) List(s *data.Store) ([]MappingEntry, error) {
	kb := data.NewKeyBuilder()

	kb.PutByte(data.KeyPrefixAttributesMapping).PutByte(a.prefix)
	lowerBound := kb.Snapshot()

	// Upper bound: same prefix + 0xFF to get all entries
	kb.PutByte(0xFF)
	upperBound := kb.Build()

	iter, err := s.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for attributes mapping: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var entries []MappingEntry

	// Key structure: [KeyPrefixAttributesMapping][prefix][canonical_key_bytes]
	// Value structure: [hash128 (16 bytes)][hash64 (8 bytes)]
	prefixLen := 2 // KeyPrefixAttributesMapping + prefix

	for iter.First(); iter.Valid(); iter.Next() {
		iterKey := iter.Key()
		if len(iterKey) <= prefixLen {
			continue // Skip invalid keys
		}

		// Extract canonical key bytes (after the prefix)
		canonicalBytes := make([]byte, len(iterKey)-prefixLen)
		copy(canonicalBytes, iterKey[prefixLen:])

		// Read value (24 bytes: hash128 + hash64)
		valueBytes, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading attribute mapping value: %w", err)
		}
		if len(valueBytes) != 24 {
			return nil, fmt.Errorf("invalid attribute mapping value length: expected 24, got %d", len(valueBytes))
		}

		// Parse hash128 (big-endian)
		hash128Hi := binary.BigEndian.Uint64(valueBytes[0:8])
		hash128Lo := binary.BigEndian.Uint64(valueBytes[8:16])
		// Parse hash64 (big-endian)
		hash64 := binary.BigEndian.Uint64(valueBytes[16:24])

		entries = append(entries, MappingEntry{
			Hash128:      NewU128(hash128Hi, hash128Lo),
			Hash64:       hash64,
			CanonicalKey: canonicalBytes,
		})
	}

	return entries, nil
}
