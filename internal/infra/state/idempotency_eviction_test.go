package state

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestIdempotencyEviction_SameTimestampSiblingsNeverOrphaned is the
// regression coverage for the batch-boundary bug flagged on PR #208.
//
// The time index key is [zone(1)][sub(1)][created_at(8)][hash(16)].
// Multiple entries can share the same created_at. Before the fix, the
// FSM's DeleteRange upper bound was derived from the timestamp alone:
// rangeEnd = [zone][sub][created_at+1]. If the leader scan stopped at
// maxKeys in the middle of a group of siblings sharing a timestamp,
// the range delete would purge their time-index entries even though
// only the first N hashes were in the proposal. The unscanned main
// keys would stay in Pebble, with no time-index entry to find them
// again — orphaned forever.
//
// The fix carries the full 26-byte time-index key of the last scanned
// entry in the proposal and uses lex-next(key) (append 0x00) as the
// DeleteRange upper bound. That bound is strictly less than any
// unscanned sibling, so the range never touches them.
func TestIdempotencyEviction_SameTimestampSiblingsNeverOrphaned(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	// Insert 5 entries that all share the same created_at and 1 entry at an
	// earlier timestamp (so we can verify it is also evicted).
	const sharedTs uint64 = 1_000_000
	const earlierTs uint64 = sharedTs - 1

	keys := []string{
		"key-1",
		"key-2",
		"key-3",
		"key-4",
		"key-5",
		"key-earlier", // earlier timestamp
	}

	batch := store.NewBatch()

	for _, k := range keys {
		ts := sharedTs
		if k == "key-earlier" {
			ts = earlierTs
		}

		require.NoError(t, saveIdempotencyKey(batch, k, &commonpb.IdempotencyKeyValue{
			CreatedAt: ts,
		}))
	}

	require.NoError(t, batch.Commit())

	idemp := NewIdempotencyStore(60_000_000)

	// Scan with maxKeys=3. The earliest 3 entries are returned in lex order:
	// the single earlierTs entry first, then the two lex-smallest hashes at
	// sharedTs. The remaining 3 siblings at sharedTs are unscanned.
	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	hashes, lastScannedKey, err := idemp.ScanExpiredKeyHashes(handle, sharedTs+1, 3)
	require.NoError(t, err)
	_ = handle.Close()

	require.Len(t, hashes, 3, "scanner must return exactly maxKeys")
	require.NotEmpty(t, lastScannedKey, "scanner must return the last scanned key")
	require.Len(t, lastScannedKey, 2+8+16, "last scanned key must be the full 26-byte time-index key")

	// The last scanned key must be at sharedTs (the scan crossed into the
	// shared-timestamp group; the earlier-ts entry sorts first by timestamp).
	lastTs := binary.BigEndian.Uint64(lastScannedKey[2:10])
	require.Equal(t, sharedTs, lastTs,
		"the scan should stop mid-sharedTs group, exercising the boundary case")

	// Apply the eviction.
	evictBatch := store.NewBatch()
	evicted, err := idemp.Evict(evictBatch, sharedTs+1, lastScannedKey, hashes)
	require.NoError(t, err)
	require.NoError(t, evictBatch.Commit())
	require.GreaterOrEqual(t, evicted, 3)

	// Now verify: the UNSCANNED siblings at sharedTs must still have BOTH
	// their main key AND their time-index entry. Anything less and a future
	// eviction can never find them.
	postHandle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = postHandle.Close() }()

	mainKeysRemaining := 0
	timeIndexRemaining := 0

	mainIter, err := postHandle.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneIdempotency, dal.SubIdempKeys},
		UpperBound: []byte{dal.ZoneIdempotency, dal.SubIdempKeys + 1},
	})
	require.NoError(t, err)
	for mainIter.First(); mainIter.Valid(); mainIter.Next() {
		mainKeysRemaining++
	}
	require.NoError(t, mainIter.Close())

	timeIter, err := postHandle.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneIdempotency, dal.SubIdempTimeIdx},
		UpperBound: []byte{dal.ZoneIdempotency, dal.SubIdempTimeIdx + 1},
	})
	require.NoError(t, err)

	for timeIter.First(); timeIter.Valid(); timeIter.Next() {
		timeIndexRemaining++
	}

	require.NoError(t, timeIter.Close())

	// 3 entries were scanned and evicted. 3 siblings at sharedTs remain.
	// Both stores must agree: 3 main keys AND 3 time-index entries.
	require.Equal(t, 3, mainKeysRemaining,
		"3 unscanned sibling main keys must still be present in Pebble")
	require.Equal(t, 3, timeIndexRemaining,
		"3 unscanned sibling time-index entries must still be present — "+
			"if this is 0 the siblings are orphaned (the bug this test guards)")
}

// TestIdempotencyEviction_LastScannedKeyExcludesSiblingsLexically is a
// finer-grained assertion on the key itself: the DeleteRange upper bound
// (lex-next of the last scanned key) must be strictly less than every
// sibling key at the same timestamp that the scanner did not include.
func TestIdempotencyEviction_LastScannedKeyExcludesSiblingsLexically(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	const ts uint64 = 42
	batch := store.NewBatch()

	for i := range 4 {
		key := []byte{byte('a' + i)}
		require.NoError(t, saveIdempotencyKey(batch, string(key), &commonpb.IdempotencyKeyValue{CreatedAt: ts}))
	}

	require.NoError(t, batch.Commit())

	idemp := NewIdempotencyStore(0)

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	hashes, lastScannedKey, err := idemp.ScanExpiredKeyHashes(handle, ts+1, 2)
	require.NoError(t, err)
	require.Len(t, hashes, 2)

	rangeEnd := append([]byte(nil), lastScannedKey...)
	rangeEnd = append(rangeEnd, 0x00)

	// Inspect every unscanned time-index key and assert rangeEnd < it.
	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: rangeEnd,
		UpperBound: []byte{dal.ZoneIdempotency, dal.SubIdempTimeIdx + 1},
	})
	require.NoError(t, err)
	defer func() { _ = iter.Close() }()

	unscannedCount := 0

	for iter.First(); iter.Valid(); iter.Next() {
		unscannedCount++

		k := iter.Key()
		require.Equal(t, 1, bytes.Compare(k, lastScannedKey),
			"every key past rangeEnd must be lex-greater than lastScannedKey")
	}

	require.NoError(t, iter.Error())
	require.Equal(t, 2, unscannedCount, "expected 2 unscanned siblings past the boundary")
}
