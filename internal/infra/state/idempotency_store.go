package state

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// HashIdempotencyKey returns a 16-byte hash of the idempotency key string.
// This is used as the Pebble key suffix for the dedicated 0x03 prefix.
func HashIdempotencyKey(key string) [16]byte {
	h := blake3.Sum256([]byte(key))

	var out [16]byte
	copy(out[:], h[:16])

	return out
}

// IdempotencyStore is the in-memory bridge between consecutive proposals.
// It holds recently written idempotency keys so that a key written by
// proposal N is visible to proposal N+1 even if N+1's preload ran before
// N was applied to Pebble.
type IdempotencyStore struct {
	entries   map[string]*commonpb.IdempotencyKeyValue
	ttlMicros uint64
}

// NewIdempotencyStore creates a new IdempotencyStore.
// ttlMicros is the time-to-live in HLC microseconds (0 = no expiration).
func NewIdempotencyStore(ttlMicros uint64) *IdempotencyStore {
	return &IdempotencyStore{
		entries:   make(map[string]*commonpb.IdempotencyKeyValue),
		ttlMicros: ttlMicros,
	}
}

// Get returns the idempotency value for the given key, if present in the in-memory map.
func (s *IdempotencyStore) Get(key string) (*commonpb.IdempotencyKeyValue, bool) {
	v, ok := s.entries[key]

	return v, ok
}

// Put writes an idempotency key to the in-memory map.
func (s *IdempotencyStore) Put(key string, value *commonpb.IdempotencyKeyValue) {
	s.entries[key] = value
}

// IsExpired returns true if the value's created_at is older than TTL relative to nowMicros.
// Returns false if TTL is 0 (no expiration).
func (s *IdempotencyStore) IsExpired(value *commonpb.IdempotencyKeyValue, nowMicros uint64) bool {
	if s.ttlMicros == 0 {
		return false
	}

	return nowMicros-value.GetCreatedAt() > s.ttlMicros
}

// Reset clears the in-memory map (used during snapshot restore).
func (s *IdempotencyStore) Reset() {
	s.entries = make(map[string]*commonpb.IdempotencyKeyValue)
}

// EvictBefore removes entries with created_at <= cutoffMicros from both
// the in-memory map and Pebble [0x03, 0x04). Returns the count of evicted entries.
func (s *IdempotencyStore) EvictBefore(batch *dal.Batch, reader dal.PebbleReader, cutoffMicros uint64) (int, error) {
	evicted := 0

	// 1. Scan in-memory map
	for key, value := range s.entries {
		if value.GetCreatedAt() <= cutoffMicros {
			delete(s.entries, key)
			evicted++
		}
	}

	// 2. Scan Pebble [0x03, 0x04) — the time index prefix
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.KeyPrefixIdempotencyTimeIdx},
		UpperBound: []byte{dal.KeyPrefixIdempotencyTimeIdx + 1},
	})
	if err != nil {
		return evicted, fmt.Errorf("creating idempotency time index iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	// Time index key format: [0x04][created_at BE 8 bytes][key_hash 16 bytes]
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		// Minimum key length: 1 (prefix) + 8 (created_at) + 16 (key hash)
		if len(k) < 25 {
			continue
		}

		createdAt := binary.BigEndian.Uint64(k[1:9])
		if createdAt > cutoffMicros {
			break // time index is sorted; all remaining entries are newer
		}

		keyHash := k[9:25]

		// Delete from main store [0x03][key_hash]
		mainKey := make([]byte, 1+16)
		mainKey[0] = dal.KeyPrefixIdempotency
		copy(mainKey[1:], keyHash)

		if err := batch.DeleteKey(mainKey); err != nil {
			return evicted, fmt.Errorf("deleting idempotency key: %w", err)
		}

		// Delete time index entry
		if err := batch.DeleteKey(k); err != nil {
			return evicted, fmt.Errorf("deleting idempotency time index: %w", err)
		}

		evicted++
	}

	if err := iter.Error(); err != nil {
		return evicted, fmt.Errorf("iterating idempotency time index: %w", err)
	}

	return evicted, nil
}

// SaveIdempotencyKey writes an idempotency key-value pair to Pebble under prefix 0x03,
// and creates a time index entry under prefix 0x04 for efficient eviction.
func SaveIdempotencyKey(batch *dal.Batch, key string, value *commonpb.IdempotencyKeyValue) error {
	keyHash := HashIdempotencyKey(key)

	// Main entry: [0x03][key_hash 16 bytes] -> marshaled IdempotencyKeyValue
	mainKey := make([]byte, 1+16)
	mainKey[0] = dal.KeyPrefixIdempotency
	copy(mainKey[1:], keyHash[:])

	data, err := value.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling idempotency value: %w", err)
	}

	if err := batch.SetBytes(mainKey, data); err != nil {
		return fmt.Errorf("writing idempotency key: %w", err)
	}

	// Time index: [0x04][created_at BE 8 bytes][key_hash 16 bytes] -> empty
	timeKey := make([]byte, 1+8+16)
	timeKey[0] = dal.KeyPrefixIdempotencyTimeIdx
	binary.BigEndian.PutUint64(timeKey[1:9], value.GetCreatedAt())
	copy(timeKey[9:], keyHash[:])

	if err := batch.SetBytes(timeKey, nil); err != nil {
		return fmt.Errorf("writing idempotency time index: %w", err)
	}

	return nil
}

// LoadIdempotencyKey reads an idempotency key from Pebble under prefix 0x03.
// Returns nil if the key does not exist.
func LoadIdempotencyKey(reader dal.PebbleReader, key string) (*commonpb.IdempotencyKeyValue, error) {
	keyHash := HashIdempotencyKey(key)

	pebbleKey := make([]byte, 1+16)
	pebbleKey[0] = dal.KeyPrefixIdempotency
	copy(pebbleKey[1:], keyHash[:])

	val, closer, err := reader.Get(pebbleKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("reading idempotency key: %w", err)
	}

	defer func() { _ = closer.Close() }()

	value := &commonpb.IdempotencyKeyValue{}
	if err := value.UnmarshalVT(val); err != nil {
		return nil, fmt.Errorf("unmarshaling idempotency value: %w", err)
	}

	return value, nil
}
