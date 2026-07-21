package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

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

	idemp := NewIdempotencyStore(60_000_000)
	batch := store.OpenWriteSession()

	for _, k := range keys {
		ts := sharedTs
		if k == "key-earlier" {
			ts = earlierTs
		}

		value := &commonpb.IdempotencyKeyValue{CreatedAt: ts}
		require.NoError(t, SaveIdempotencyKey(batch, k, value))
		// Mirror the in-memory map: production paths always Put alongside
		// the Pebble write, and RestoreFromStore rebuilds the map from
		// Pebble at boot. Evict's step-2 dedup uses the map as the source
		// of authority for which hashes still need a SingleDelete.
		idemp.Put(k, value)
	}

	require.NoError(t, batch.Commit())

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
	evictBatch := store.OpenWriteSession()
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
	idemp := NewIdempotencyStore(0)
	batch := store.OpenWriteSession()

	for i := range 4 {
		key := []byte{byte('a' + i)}
		value := &commonpb.IdempotencyKeyValue{CreatedAt: ts}
		require.NoError(t, SaveIdempotencyKey(batch, string(key), value))
		idemp.Put(string(key), value)
	}

	require.NoError(t, batch.Commit())

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

// TestIdempotencyEviction_DoubleApplyIsNoOp guards the dedup logic added in
// Evict against a scheduler retry-on-timeout race: when the bounded-timeout
// version of proposeTechnical returned before Raft applied the first
// proposal, the next tick re-scanned the same Pebble entries and proposed
// the same hashes a second time. Both applied in series, and the FSM's
// SingleDeleteKey loop would have called SingleDelete twice on the same
// Pebble main key — undefined per Pebble's SingleDelete contract.
//
// The dedup uses the in-memory map as the source of authority: step 2 only
// emits a SingleDelete for hashes that step 1 actually evicted from the
// map. A duplicate-payload apply therefore sees the map already empty for
// these hashes and skips every SingleDelete; the observable signal is
// `evicted == 0` on the second call.
func TestIdempotencyEviction_DoubleApplyIsNoOp(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	const ts uint64 = 1_000_000

	keys := []string{"a", "b", "c"}
	idemp := NewIdempotencyStore(0)

	batch := store.OpenWriteSession()
	for _, k := range keys {
		value := &commonpb.IdempotencyKeyValue{CreatedAt: ts}
		require.NoError(t, SaveIdempotencyKey(batch, k, value))
		idemp.Put(k, value)
	}

	require.NoError(t, batch.Commit())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	hashes, lastScannedKey, err := idemp.ScanExpiredKeyHashes(handle, ts+1, 10)
	require.NoError(t, err)
	_ = handle.Close()
	require.Len(t, hashes, 3)

	// First apply: normal eviction path. All three entries leave the map
	// and a SingleDelete is emitted per hash.
	evictBatch1 := store.OpenWriteSession()
	evicted1, err := idemp.Evict(evictBatch1, ts+1, lastScannedKey, hashes)
	require.NoError(t, err)
	require.NoError(t, evictBatch1.Commit())
	require.Equal(t, 3, evicted1, "first eviction must remove all 3 entries")

	// Second apply with the SAME hashes simulates the scheduler retry-on-
	// timeout window. The map is already empty for these hashes, so step 2
	// must skip every SingleDelete — observable via evicted == 0.
	evictBatch2 := store.OpenWriteSession()
	evicted2, err := idemp.Evict(evictBatch2, ts+1, lastScannedKey, hashes)
	require.NoError(t, err)
	require.NoError(t, evictBatch2.Commit())
	require.Equal(t, 0, evicted2,
		"second eviction with same hashes must be a no-op — re-emitting "+
			"SingleDelete on an already-deleted Pebble main key violates "+
			"the SingleDelete write-once/delete-once contract")
}

// TestIdempotencyEviction_MultiBatchConvergence is the regression coverage
// for the orphaning bug flagged on NumaryBot's review of the in-memory
// dedup: when more expired entries exist than maxEvictionBatchSize, a
// single Evict call must NOT evict from the in-memory map any entry it
// did not also SingleDelete from Pebble. Otherwise the second batch sees
// those entries already absent from the map and skips the SingleDelete —
// the unscanned Pebble main keys would be orphaned forever.
//
// The test simulates the multi-batch eviction sequence: insert 4 expired
// entries, scan with maxKeys=2 (mimicking the scheduler's bounded scan),
// apply, then re-scan and re-apply. After both batches every main key
// must be deleted and the map must be empty.
func TestIdempotencyEviction_MultiBatchConvergence(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	const ts uint64 = 100

	idemp := NewIdempotencyStore(0)
	batch := store.OpenWriteSession()

	// Use distinct timestamps per key to make the time-index ordering
	// (and the bounded scan) deterministic.
	for i := range 4 {
		key := []byte{byte('a' + i)}
		value := &commonpb.IdempotencyKeyValue{CreatedAt: ts + uint64(i)}
		require.NoError(t, SaveIdempotencyKey(batch, string(key), value))
		idemp.Put(string(key), value)
	}

	require.NoError(t, batch.Commit())

	cutoff := ts + 10

	// --- Batch 1: scan 2 of 4, apply.
	h1, err := store.NewReadHandle()
	require.NoError(t, err)

	hashes1, lastScannedKey1, err := idemp.ScanExpiredKeyHashes(h1, cutoff, 2)
	require.NoError(t, err)
	_ = h1.Close()
	require.Len(t, hashes1, 2)

	wb1 := store.OpenWriteSession()
	evicted1, err := idemp.Evict(wb1, cutoff, lastScannedKey1, hashes1)
	require.NoError(t, err)
	require.NoError(t, wb1.Commit())
	require.Equal(t, 2, evicted1, "batch 1 must evict exactly the 2 scanned hashes")

	// The 2 unscanned entries MUST still be in the map; otherwise the
	// next scan would find their time-index entries but the apply would
	// observe an empty map and skip the SingleDelete, orphaning the main
	// keys forever (the bug this test guards).
	mapAfterB1 := idemp.entries
	require.Len(t, mapAfterB1, 2,
		"unscanned entries must remain in the map so batch 2's apply "+
			"can still authorize their SingleDelete")

	// --- Batch 2: scan the remaining 2, apply.
	h2, err := store.NewReadHandle()
	require.NoError(t, err)

	hashes2, lastScannedKey2, err := idemp.ScanExpiredKeyHashes(h2, cutoff, 2)
	require.NoError(t, err)
	_ = h2.Close()
	require.Len(t, hashes2, 2, "the 2 unscanned entries must still be reachable via the time-index")

	wb2 := store.OpenWriteSession()
	evicted2, err := idemp.Evict(wb2, cutoff, lastScannedKey2, hashes2)
	require.NoError(t, err)
	require.NoError(t, wb2.Commit())
	require.Equal(t, 2, evicted2, "batch 2 must evict its 2 hashes via SingleDelete")

	// --- Verify final state: no main keys, no time-index entries, empty map.
	post, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = post.Close() }()

	mainIter, err := post.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneIdempotency, dal.SubIdempKeys},
		UpperBound: []byte{dal.ZoneIdempotency, dal.SubIdempKeys + 1},
	})
	require.NoError(t, err)

	mainRemaining := 0

	for mainIter.First(); mainIter.Valid(); mainIter.Next() {
		mainRemaining++
	}

	require.NoError(t, mainIter.Close())
	require.Equal(t, 0, mainRemaining,
		"all 4 main keys must be deleted across both batches — any "+
			"survivor here is an orphaned key the in-memory dedup leaked")

	timeIter, err := post.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneIdempotency, dal.SubIdempTimeIdx},
		UpperBound: []byte{dal.ZoneIdempotency, dal.SubIdempTimeIdx + 1},
	})
	require.NoError(t, err)

	timeRemaining := 0

	for timeIter.First(); timeIter.Valid(); timeIter.Next() {
		timeRemaining++
	}

	require.NoError(t, timeIter.Close())
	require.Equal(t, 0, timeRemaining, "time-index must be fully drained")

	require.Empty(t, idemp.entries, "in-memory map must be empty after both batches")
}

// TestIdempotencyEvictionScheduler_StopCancelsProposeFn guards the
// stop-cancelled context pattern that replaced the previous 30s timeout in
// the callback. The scheduler builds a context from its stop signal and
// passes it to proposeFn; Stop() must cancel that context so an in-flight
// callback unblocks instead of pinning the worker — and so no spurious
// timeout-driven retry can submit a duplicate IdempotencyEviction with the
// same hashes after Raft has already accepted the first one.
func TestIdempotencyEvictionScheduler_StopCancelsProposeFn(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	// Seed one expired entry so the scheduler's first tick has something
	// to propose (otherwise proposeFn is never invoked).
	const expiredTs uint64 = 1

	idemp := NewIdempotencyStore(0)
	batch := store.OpenWriteSession()
	value := &commonpb.IdempotencyKeyValue{CreatedAt: expiredTs}
	require.NoError(t, SaveIdempotencyKey(batch, "stop-test", value))
	idemp.Put("stop-test", value)
	require.NoError(t, batch.Commit())

	logger := logging.FromContext(logging.TestingContext())

	var (
		callbackErr atomic.Value // stores ctx.Err() observed by the callback
		started     = make(chan struct{})
	)

	scheduler := NewIdempotencyEvictionScheduler(
		logger,
		func() bool { return true },
		func(ctx context.Context, _ uint64, _ []byte, _ [][]byte) {
			// Signal the test that the callback fired, then block until
			// the scheduler cancels ctx via Stop().
			select {
			case <-started: // already closed → another tick fired; just block again
			default:
				close(started)
			}

			<-ctx.Done()
			callbackErr.Store(ctx.Err())
		},
		store,
		idemp,
		10*time.Millisecond,
		0, // ttl=0 ⇒ every entry counts as expired given expiredTs above
	)

	scheduler.Start()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler callback never fired — proposeFn was not invoked")
	}

	stopDone := make(chan struct{})

	go func() {
		scheduler.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler.Stop() did not return — proposeFn ctx was not cancelled")
	}

	got, ok := callbackErr.Load().(error)
	require.True(t, ok, "callback never observed ctx.Done()")
	require.True(t, errors.Is(got, context.Canceled),
		"proposeFn ctx must be cancelled by Stop, got %v", got)
}
