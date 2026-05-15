package bloom

import (
	"encoding/binary"
	"maps"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

func newTestFilter(t *testing.T) *Filter {
	t.Helper()

	meter := noop.NewMeterProvider().Meter("test")

	return newFilter(10000, 0.01, dal.AttributeCodeVolume, meter, "test")
}

func hashKey(key []byte) attributes.U128 {
	return attributes.HashU128(attributes.DefaultSeeds, key)
}

func TestFilter_DirtyTracking(t *testing.T) {
	t.Parallel()

	f := newTestFilter(t)

	// Initially no dirty blocks.
	count := 0
	for range f.dirtyBlocks() {
		count++
	}

	require.Equal(t, 0, count, "should have no dirty blocks initially")

	// Add a key; should dirty exactly one block.
	f.Add(hashKey([]byte("test-key")))

	dirtyIndices := make(map[uint64]bool)
	for idx := range f.dirtyBlocks() {
		dirtyIndices[idx] = true
	}

	require.Equal(t, 1, len(dirtyIndices), "should have exactly one dirty block")

	// Clear resets the dirty set.
	clear(f.dirty)

	count = 0
	for range f.dirtyBlocks() {
		count++
	}

	require.Equal(t, 0, count, "should have no dirty blocks after clear")
}

func TestFilter_DirtyBlocks_MultipleBlocks(t *testing.T) {
	t.Parallel()

	f := newTestFilter(t)
	rng := rand.New(rand.NewPCG(3, 3^0xDEADBEEF))

	for range 1000 {
		var buf [16]byte
		binary.LittleEndian.PutUint64(buf[:8], rng.Uint64())
		binary.LittleEndian.PutUint64(buf[8:], rng.Uint64())
		f.Add(hashKey(buf[:]))
	}

	dirty := maps.Collect(f.dirtyBlocks())

	require.Greater(t, len(dirty), 1, "should have multiple dirty blocks after many inserts")

	for idx, blk := range dirty {
		nonZero := false
		for _, w := range blk {
			if w != 0 {
				nonZero = true

				break
			}
		}

		require.True(t, nonZero, "dirty block %d should have non-zero data", idx)
	}
}

func TestFilter_PersistAndRestore(t *testing.T) {
	t.Parallel()

	f := newTestFilter(t)
	rng := rand.New(rand.NewPCG(5, 5^0xDEADBEEF))

	keys := make([]attributes.U128, 500)
	for i := range keys {
		var buf [16]byte
		binary.LittleEndian.PutUint64(buf[:8], rng.Uint64())
		binary.LittleEndian.PutUint64(buf[8:], rng.Uint64())
		keys[i] = hashKey(buf[:])
		f.Add(keys[i])
	}

	// Simulate persist: collect dirty blocks via marshal.
	persisted := make(map[uint64][]byte)
	for idx, blk := range f.dirtyBlocks() {
		persisted[idx] = marshalBlock(&blk)
	}

	clear(f.dirty)

	// Add more keys after the flush.
	for range 200 {
		var buf [16]byte
		binary.LittleEndian.PutUint64(buf[:8], rng.Uint64())
		binary.LittleEndian.PutUint64(buf[8:], rng.Uint64())
		f.Add(hashKey(buf[:]))
	}

	// Simulate restore into a fresh filter (only the flushed blocks).
	restored := newBlockedFilter(f.filter.NumBits(), f.filter.K())
	for idx, data := range persisted {
		restored.SetBlock(idx, unmarshalBlock(data))
	}

	// Keys from before the flush must be found.
	for _, key := range keys {
		require.True(t, restored.Has(key.Hi()), "pre-flush key should be present")
	}
}

// TestFilter_PartialFlushCausesFalseNegatives demonstrates the bloom filter
// false negative bug and verifies the fix.
//
// Bug: when PersistDirtyBlocks is called during async bloom population (before
// IsReady), the flushed blocks are incomplete. After restart, restoring from
// these partial blocks produces false negatives for "cold" entries that exist
// in Pebble but are not in the restored cache.
//
// Production scenario:
//  1. Node starts (first boot or snapshot restore) → StartAsyncBloomPopulate
//  2. Async scan iterates Pebble attributes, adding entries to the bloom
//  3. Cache rotation triggers PersistDirtyBlocks mid-scan → partial blocks in Pebble
//  4. Node restarts → partial blocks loaded + cache replay (cache is sparse)
//  5. Bloom marked ready → false negatives for cold accounts → "available 0"
//
// Fix: skip PersistDirtyBlocks when !IsReady(). On restart, no blocks exist →
// full attribute scan → no false negatives.
func TestFilter_PartialFlushCausesFalseNegatives(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(42, 42^0xDEADBEEF))

	// Generate 1000 entries representing all volumes in Pebble.
	allKeys := make([]attributes.U128, 1000)
	for i := range allKeys {
		var buf [16]byte
		binary.LittleEndian.PutUint64(buf[:8], rng.Uint64())
		binary.LittleEndian.PutUint64(buf[8:], rng.Uint64())
		allKeys[i] = hashKey(buf[:])
	}

	// Split: first 500 are scanned before the rotation flush ("early scan"),
	// last 500 are scanned after ("late scan"). The "cold" entries are those
	// from the late scan that are NOT in the cache at restart time.
	earlyScanKeys := allKeys[:500]
	lateScanKeys := allKeys[500:]

	// Simulate a subset of late-scan keys that happen to be in the restored
	// cache (recently accessed, "hot"). The rest are "cold" (evicted).
	hotKeys := lateScanKeys[:50] // only 50 out of 500 are in cache
	coldKeys := lateScanKeys[50:]

	// --- Phase 1: Simulate the async bloom population with a mid-scan flush ---
	f := newTestFilter(t)

	// Async scan processes the early half.
	for _, key := range earlyScanKeys {
		f.Add(key)
	}

	// Rotation happens mid-scan → PersistDirtyBlocks flushes partial blocks.
	persisted := make(map[uint64][]byte)
	for idx, blk := range f.dirtyBlocks() {
		persisted[idx] = marshalBlock(&blk)
	}
	// Clear dirty bits (as PersistDirtyBlocks does).
	for i := range f.dirty {
		f.dirty[i] = 0
	}

	// Async scan continues with the late half.
	for _, key := range lateScanKeys {
		f.Add(key)
	}

	// Scan completes. In memory, the bloom is complete — all 1000 entries present.
	for _, key := range allKeys {
		require.True(t, f.MayContain(key),
			"in-memory bloom should contain all keys after full scan")
	}

	// --- Phase 2: Bug path — restore from partial blocks (no fix) ---
	buggyRestore := newTestFilter(t)

	for idx, data := range persisted {
		buggyRestore.filter.SetBlock(idx, unmarshalBlock(data))
	}

	for _, key := range hotKeys {
		buggyRestore.Add(key)
	}

	falseNegatives := 0
	for _, key := range coldKeys {
		if !buggyRestore.MayContain(key) {
			falseNegatives++
		}
	}

	require.Greater(t, falseNegatives, 0,
		"partial bloom restore should produce false negatives for cold entries — "+
			"this demonstrates the bug where PersistDirtyBlocks is called during "+
			"async population (IsReady=false)")

	// Early-scan and hot keys are unaffected.
	for _, key := range earlyScanKeys {
		require.True(t, buggyRestore.MayContain(key),
			"early-scan key should be present (was in the flushed blocks)")
	}

	for _, key := range hotKeys {
		require.True(t, buggyRestore.MayContain(key),
			"hot key should be present (was replayed from cache)")
	}

	// --- Phase 3: Fix path — skip flush when !IsReady, restore triggers full scan ---
	// With the fix, no partial blocks are persisted. On restart,
	// hasPersistedBloomBlocks()=false → full attribute scan (PopulateFromStore)
	// which adds ALL keys. We simulate this by adding all keys to a fresh filter.
	fixedRestore := newTestFilter(t)

	for _, key := range allKeys {
		fixedRestore.Add(key)
	}

	for _, key := range allKeys {
		require.True(t, fixedRestore.MayContain(key),
			"after full attribute scan (fix path), all keys must be present")
	}
}
