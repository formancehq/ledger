package state

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// HashIdempotencyKey returns a 128-bit hash of the idempotency key string,
// used both as the Pebble key suffix under `[0x05][0x01]` and as the
// in-memory map key.
func HashIdempotencyKey(key string) attributes.U128 {
	h := blake3.Sum256([]byte(key))

	return attributes.U128FromBytes(h[:16])
}

// IdempotencyStore is the in-memory bridge between consecutive proposals.
// It holds recently written idempotency keys so that a key written by
// proposal N is visible to proposal N+1 even if N+1's preload ran before
// N was applied to Pebble.
//
// The map is keyed by the same U128 blake3 hash used as the Pebble key
// suffix. This lets the boot-time RestoreFromStore scan rebuild the map
// directly from `[0x05][0x01]` Pebble entries without needing the original
// caller-supplied string, and keeps lookups O(1) on the hash.
type IdempotencyStore struct {
	entries   map[attributes.U128]*commonpb.IdempotencyKeyValue
	ttlMicros uint64
}

// NewIdempotencyStore creates a new IdempotencyStore.
// ttlMicros is the time-to-live in HLC microseconds (0 = no expiration).
func NewIdempotencyStore(ttlMicros uint64) *IdempotencyStore {
	return &IdempotencyStore{
		entries:   make(map[attributes.U128]*commonpb.IdempotencyKeyValue),
		ttlMicros: ttlMicros,
	}
}

// Get returns the idempotency value for the given key, if present in the in-memory map.
func (s *IdempotencyStore) Get(key string) (*commonpb.IdempotencyKeyValue, bool) {
	v, ok := s.entries[HashIdempotencyKey(key)]

	return v, ok
}

// Put writes an idempotency key to the in-memory map.
func (s *IdempotencyStore) Put(key string, value *commonpb.IdempotencyKeyValue) {
	s.entries[HashIdempotencyKey(key)] = value
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
	s.entries = make(map[attributes.U128]*commonpb.IdempotencyKeyValue)
}

// RestoreFromStore rebuilds the in-memory map from Pebble. It scans every
// entry under `[0x05][0x01]` (the idempotency main-key zone), extracts the
// 16-byte hash from the Pebble key, unmarshals the value, and repopulates the
// map.
//
// No TTL filter is applied: the map MUST be identical across all nodes for
// the same applied index (Cache-is-source-of-authority invariant). A wall-
// clock TTL filter at boot time would yield different maps on nodes that
// restart at different moments. Removing stale entries is exclusively the
// job of the Raft-replicated IdempotencyEviction command, which uses a
// deterministic cutoff embedded in the proposal.
func (s *IdempotencyStore) RestoreFromStore(reader dal.PebbleReader) error {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneIdempotency, dal.SubIdempKeys},
		UpperBound: []byte{dal.ZoneIdempotency, dal.SubIdempKeys + 1},
	})
	if err != nil {
		return fmt.Errorf("creating idempotency main-key iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	// Main key layout: [0x05][0x01][hash 16 bytes] (length = 18).
	const mainKeyLen = 2 + 16

	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		if len(k) != mainKeyLen {
			continue
		}

		value := &commonpb.IdempotencyKeyValue{}
		if err := value.UnmarshalVT(iter.Value()); err != nil {
			return fmt.Errorf("unmarshaling idempotency value: %w", err)
		}

		s.entries[attributes.U128FromBytes(k[2:mainKeyLen])] = value
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterating idempotency main keys: %w", err)
	}

	return nil
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
//
// The in-memory map (s.entries) is the source of authority for which hashes
// have already been evicted: cache and Pebble stay in sync (entries enter via
// Put, exit via this Evict, and RestoreFromStore rebuilds the map from
// Pebble). The step-2 SingleDelete loop therefore skips any hash absent from
// the map at the start of THIS apply — that hash was already deleted by a
// previous apply, and a second SingleDelete on the same Pebble main key
// would violate Pebble's write-once/delete-once SingleDelete contract
// (resulting state is undefined, can resurrect the value at compaction time).
//
// This dedup matters because the leader-side scheduler bounds proposeTechnical
// with a context timeout: if Raft accepts a proposal but the FSM apply lags
// past that timeout, the scheduler logs the error and on the next tick
// re-scans the same expired Pebble entries (the first proposal has not yet
// applied), then submits a second proposal with the same hashes. Both apply
// in series; without this dedup the second apply would re-SingleDelete every
// main key.
func (s *IdempotencyStore) Evict(batch *dal.WriteSession, cutoffMicros uint64, lastScannedTimeIndexKey []byte, pebbleKeyHashes [][]byte) (int, error) {
	evicted := 0

	// Walk the pre-scanned hashes and evict each from the in-memory map
	// alongside its Pebble main key. Two important properties:
	//
	//   * We iterate over `pebbleKeyHashes`, NOT over `s.entries`. When the
	//     leader scan caps at maxEvictionBatchSize, the proposal only
	//     covers the K oldest expired hashes; the remaining expired
	//     entries must stay in the map (and in Pebble) so the next tick's
	//     scan finds them. Evicting them from the map here, while their
	//     Pebble main keys still exist, would orphan them: the next apply
	//     would see them absent from the map and skip the SingleDelete
	//     (see the dedup rule below).
	//
	//   * For each scanned hash, the SingleDelete is gated on "still
	//     present in the map". Cache and Pebble stay in sync (entries
	//     enter via Put, exit here, and RestoreFromStore rebuilds the map
	//     from Pebble), so a hash absent from the map at apply time was
	//     already evicted by a previous apply. Re-emitting SingleDelete
	//     on that main key would violate Pebble's write-once/delete-once
	//     SingleDelete contract (undefined result — value can resurrect
	//     at compaction time). This shields the FSM against a scheduler
	//     retry that re-submits the same hashes after Raft has accepted
	//     the first proposal but its apply has not yet landed.
	for _, keyHash := range pebbleKeyHashes {
		u128 := attributes.U128FromBytes(keyHash)

		value, ok := s.entries[u128]
		if !ok {
			// Already evicted by a previous apply — skip SingleDelete to
			// preserve the SingleDelete lifecycle.
			continue
		}

		if value.GetCreatedAt() > cutoffMicros {
			// Defensive: the leader scan returned this hash with a stale
			// cutoff. Don't evict a still-live entry.
			continue
		}

		delete(s.entries, u128)
		evicted++

		// Main keys ([0x05][0x01][hash]) are hash-ordered and must be
		// deleted individually via SingleDelete (write-once / delete-once
		// lifecycle guaranteed by the FSM).
		mainKey := make([]byte, 2+16)
		mainKey[0] = dal.ZoneIdempotency
		mainKey[1] = dal.SubIdempKeys
		copy(mainKey[2:], keyHash)

		if err := batch.SingleDeleteKey(mainKey); err != nil {
			return evicted, fmt.Errorf("deleting idempotency key: %w", err)
		}
	}

	// Bulk-delete the time-index range up to and including the last
	// scanned key. Time-index keys ([0x05][0x02][ts][hash]) are
	// timestamp-ordered, so all scanned entries form a contiguous
	// prefix; a single DeleteRange bounded by lex-next(lastScannedTime-
	// IndexKey) replaces N individual deletes, halving the batch
	// operation count and enabling Pebble's delete-only compactions
	// (entire SSTables dropped without I/O). lex-next of any key is
	// that key with a 0x00 byte appended — strictly greater than the
	// key itself but strictly less than any longer key with the same
	// prefix; this guarantees we never touch a time-index entry whose
	// main key is not in pebbleKeyHashes. DeleteRange is idempotent
	// over an already-empty range, so this step needs no per-key dedup.
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

	// `evicted` now reflects the in-memory map deletions, which by the
	// invariant above equal the Pebble main-key SingleDeletes emitted in
	// step 2. A duplicate-payload apply (race with a scheduler retry)
	// observes the map already empty for these hashes and reports 0.
	return evicted, nil
}

// SaveIdempotencyKey writes an idempotency key-value pair to Pebble under prefix 0x03,
// and creates a time index entry under prefix 0x04 for efficient eviction.
func saveIdempotencyKey(batch *dal.WriteSession, key string, value *commonpb.IdempotencyKeyValue) error {
	keyHash := HashIdempotencyKey(key)

	// Main entry: [0x05][0x01][key_hash 16 bytes] -> marshaled IdempotencyKeyValue
	mainKey := make([]byte, 2+16)
	mainKey[0] = dal.ZoneIdempotency
	mainKey[1] = dal.SubIdempKeys
	copy(mainKey[2:], keyHash[:])

	// Route through batch.SetProto so the WriteSession's deterministic mode
	// (cluster-wide fsm_determinism_enabled) controls the marshal. When the
	// flag is OFF this is byte-equivalent to MarshalVT; when ON the
	// IdempotencyKeyValue's map<string, IdempotencyAccountFieldUpdate>
	// payload is encoded with sorted map keys so two nodes persist identical
	// bytes and the cross-node digest stays aligned.
	if err := batch.SetProto(mainKey, value); err != nil {
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
	keyHash := HashIdempotencyKey(key)

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
