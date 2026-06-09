package state

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// HashIdempotencyKey returns a 16-byte hash of the idempotency key string.
// This is used as the Pebble key suffix for the dedicated 0x03 prefix.
func hashIdempotencyKey(key string) [16]byte {
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

// ScanExpiredKeyHashes reads the Pebble time index and returns up to maxKeys
// 16-byte key hashes of entries with created_at <= cutoffMicros, plus the
// full Pebble time-index key of the last scanned entry.
//
// The full key is used as an exact upper bound by the FSM's DeleteRange (via
// lex-next, append 0x00) — bounding only by timestamp is unsafe because
// multiple entries can share the same created_at: if the scan stops mid-
// timestamp, a timestamp-only upper bound deletes time-index entries whose
// main keys were NOT included in the proposal, orphaning them.
//
// This is called on the leader OUTSIDE the FSM apply path. The returned
// hashes are embedded in the Raft proposal so the FSM apply is write-only.
func (s *IdempotencyStore) ScanExpiredKeyHashes(reader dal.PebbleReader, cutoffMicros uint64, maxKeys int) ([][]byte, []byte, error) {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneIdempotency, dal.SubIdempTimeIdx},
		UpperBound: []byte{dal.ZoneIdempotency, dal.SubIdempTimeIdx + 1},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("creating idempotency time index iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	var (
		hashes         [][]byte
		lastScannedKey []byte
	)

	// Time index key format: [0x05][0x02][created_at BE 8 bytes][key_hash 16 bytes]
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		// Minimum key length: 2 (zone+sub) + 8 (created_at) + 16 (key hash)
		if len(k) < 26 {
			continue
		}

		createdAt := binary.BigEndian.Uint64(k[2:10])
		if createdAt > cutoffMicros {
			break // time index is sorted; all remaining entries are newer
		}

		hash := make([]byte, 16)
		copy(hash, k[10:26])
		hashes = append(hashes, hash)

		// Copy the full 26-byte time-index key — iter.Key() points into
		// Pebble-owned memory that may be reused on Next().
		lastScannedKey = append(lastScannedKey[:0], k...)

		if len(hashes) >= maxKeys {
			break
		}
	}

	if err := iter.Error(); err != nil {
		return nil, nil, fmt.Errorf("iterating idempotency time index: %w", err)
	}

	return hashes, lastScannedKey, nil
}

// Evict removes expired entries from the in-memory map and issues write-only
// Pebble deletes for the key hashes pre-scanned by the leader.
//
// lastScannedTimeIndexKey is the full 26-byte Pebble key of the last entry
// the leader scanned. It bounds the DeleteRange at the key level rather than
// the timestamp level: bounding by timestamp + 1 alone is unsafe because if
// the scan stops mid-timestamp, the unscanned siblings sharing that
// created_at would have their time-index entry deleted but their main key
// (not in pebbleKeyHashes) would survive — orphaning them forever. The
// DeleteRange upper bound is lex-next(lastScannedTimeIndexKey) which is
// lex-strictly-less than any unscanned sibling.
//
// This is called from the FSM apply path — no Pebble reads occur.
func (s *IdempotencyStore) Evict(batch *dal.Batch, cutoffMicros uint64, lastScannedTimeIndexKey []byte, pebbleKeyHashes [][]byte) (int, error) {
	evicted := 0

	// 1. Scan in-memory map
	for key, value := range s.entries {
		if value.GetCreatedAt() <= cutoffMicros {
			delete(s.entries, key)
			evicted++
		}
	}

	// 2. Write-only Pebble deletes using the pre-scanned key hashes.
	//
	// Main keys ([0x05][0x01][hash]) are hash-ordered and must be deleted individually
	// via SingleDelete (write-once / delete-once lifecycle guaranteed by the FSM).
	//
	// Time index keys ([0x05][0x02][ts][hash]) are timestamp-ordered, so all
	// scanned entries form a contiguous prefix. A single DeleteRange bounded
	// by lex-next(lastScannedTimeIndexKey) replaces N individual deletes,
	// halving the batch operation count and enabling Pebble's delete-only
	// compactions (entire SSTables dropped without I/O).
	for _, keyHash := range pebbleKeyHashes {
		mainKey := make([]byte, 2+16)
		mainKey[0] = dal.ZoneIdempotency
		mainKey[1] = dal.SubIdempKeys
		copy(mainKey[2:], keyHash)

		if err := batch.SingleDeleteKey(mainKey); err != nil {
			return evicted, fmt.Errorf("deleting idempotency key: %w", err)
		}
	}

	// Bulk-delete the time index range up to and including the last scanned
	// key. lex-next of any key is that key with a 0x00 byte appended — strictly
	// greater than the key itself but strictly less than any longer key with
	// the same prefix. This guarantees we never touch a time-index entry
	// whose main key is not in pebbleKeyHashes.
	if len(pebbleKeyHashes) > 0 && len(lastScannedTimeIndexKey) > 0 {
		rangeEnd := make([]byte, len(lastScannedTimeIndexKey)+1)
		copy(rangeEnd, lastScannedTimeIndexKey)
		// rangeEnd[len(lastScannedTimeIndexKey)] is already 0x00 from make.

		if err := batch.DeleteRangeNoSync(
			[]byte{dal.ZoneIdempotency, dal.SubIdempTimeIdx},
			rangeEnd,
		); err != nil {
			return evicted, fmt.Errorf("deleting idempotency time index range: %w", err)
		}
	}

	evicted += len(pebbleKeyHashes)

	return evicted, nil
}

// SaveIdempotencyKey writes an idempotency key-value pair to Pebble under prefix 0x03,
// and creates a time index entry under prefix 0x04 for efficient eviction.
func saveIdempotencyKey(batch *dal.Batch, key string, value *commonpb.IdempotencyKeyValue) error {
	keyHash := hashIdempotencyKey(key)

	// Main entry: [0x05][0x01][key_hash 16 bytes] -> marshaled IdempotencyKeyValue
	mainKey := make([]byte, 2+16)
	mainKey[0] = dal.ZoneIdempotency
	mainKey[1] = dal.SubIdempKeys
	copy(mainKey[2:], keyHash[:])

	data, err := value.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling idempotency value: %w", err)
	}

	if err := batch.SetBytes(mainKey, data); err != nil {
		return fmt.Errorf("writing idempotency key: %w", err)
	}

	// Time index: [0x05][0x02][created_at BE 8 bytes][key_hash 16 bytes] -> empty
	timeKey := make([]byte, 2+8+16)
	timeKey[0] = dal.ZoneIdempotency
	timeKey[1] = dal.SubIdempTimeIdx
	binary.BigEndian.PutUint64(timeKey[2:10], value.GetCreatedAt())
	copy(timeKey[10:], keyHash[:])

	if err := batch.SetBytes(timeKey, nil); err != nil {
		return fmt.Errorf("writing idempotency time index: %w", err)
	}

	return nil
}

// LoadIdempotencyKey reads an idempotency key from Pebble under prefix 0x03.
// Returns nil if the key does not exist.
func LoadIdempotencyKey(reader dal.PebbleReader, key string) (*commonpb.IdempotencyKeyValue, error) {
	keyHash := hashIdempotencyKey(key)

	pebbleKey := make([]byte, 2+16)
	pebbleKey[0] = dal.ZoneIdempotency
	pebbleKey[1] = dal.SubIdempKeys
	copy(pebbleKey[2:], keyHash[:])

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
