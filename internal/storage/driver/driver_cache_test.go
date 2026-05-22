package driver

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"

	ledger "github.com/formancehq/ledger/internal"
)

func newTestDriver(ttl time.Duration) *Driver {
	return &Driver{
		ledgerCache: make(map[string]cachedLedger),
		cacheGens:   make(map[string]uint64),
		cacheTTL:    ttl,
	}
}

func TestCacheHitBeforeTTL(t *testing.T) {
	d := newTestDriver(time.Minute)
	l := ledger.MustNewWithDefault("test-ledger")

	d.setCachedLedger(l)

	got, ok := d.getCachedLedger(l.Name)
	require.True(t, ok)
	require.Equal(t, l.Name, got.Name)
}

func TestCacheMissAfterTTL(t *testing.T) {
	d := newTestDriver(time.Millisecond)
	l := ledger.MustNewWithDefault("test-ledger")

	d.setCachedLedger(l)
	time.Sleep(2 * time.Millisecond)

	_, ok := d.getCachedLedger(l.Name)
	require.False(t, ok)
}

func TestCacheMissUnknownKey(t *testing.T) {
	d := newTestDriver(time.Minute)

	_, ok := d.getCachedLedger("does-not-exist")
	require.False(t, ok)
}

func TestCacheEviction(t *testing.T) {
	d := newTestDriver(time.Minute)
	l := ledger.MustNewWithDefault("test-ledger")

	d.setCachedLedger(l)
	d.evictCachedLedger(l.Name)

	_, ok := d.getCachedLedger(l.Name)
	require.False(t, ok)
}

func TestCacheDisabledWhenTTLZero(t *testing.T) {
	d := newTestDriver(0)
	l := ledger.MustNewWithDefault("test-ledger")

	d.setCachedLedger(l)

	_, ok := d.getCachedLedger(l.Name)
	require.False(t, ok)
}

func TestCacheConcurrentAccess(t *testing.T) {
	d := newTestDriver(time.Minute)
	l := ledger.MustNewWithDefault("test-ledger")

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(3)
		go func() {
			defer wg.Done()
			d.setCachedLedger(l)
		}()
		go func() {
			defer wg.Done()
			d.getCachedLedger(l.Name)
		}()
		go func() {
			defer wg.Done()
			d.evictCachedLedger(l.Name)
		}()
	}
	wg.Wait()

	// After concurrent set/get/evict, the cache must be in a consistent state:
	// either empty (evict won last) or holding the correct ledger (set won last).
	// Any other value indicates corruption.
	got, ok := d.getCachedLedger(l.Name)
	if ok {
		require.Equal(t, l.Name, got.Name, "cached ledger must equal the one that was written")
	}
	// Both outcomes (present or absent) are valid; no panic and no corrupted
	// state is the invariant this test enforces under -race.
}

func TestCacheIsolationBetweenLedgers(t *testing.T) {
	d := newTestDriver(time.Minute)
	l1 := ledger.MustNewWithDefault("ledger-one")
	l2 := ledger.MustNewWithDefault("ledger-two")

	d.setCachedLedger(l1)
	d.setCachedLedger(l2)

	d.evictCachedLedger(l1.Name)

	_, ok1 := d.getCachedLedger(l1.Name)
	require.False(t, ok1, "evicted ledger should not be present")

	got2, ok2 := d.getCachedLedger(l2.Name)
	require.True(t, ok2, "non-evicted ledger should still be present")
	require.Equal(t, l2.Name, got2.Name)
}

func TestCacheEvictNonExistentKey(t *testing.T) {
	d := newTestDriver(time.Minute)
	require.NotPanics(t, func() {
		d.evictCachedLedger("does-not-exist")
	})
}

func TestCacheEvictionDoesNotBumpGenWhenDisabled(t *testing.T) {
	d := newTestDriver(0) // caching disabled

	// Repeated evictions must not accumulate entries in cacheGens.
	d.evictCachedLedger("test-ledger")
	d.evictCachedLedger("test-ledger")

	d.mu.RLock()
	_, exists := d.cacheGens["test-ledger"]
	d.mu.RUnlock()

	require.False(t, exists, "cacheGens must not accumulate entries when cache is disabled")
}

func TestCacheUpdateEntry(t *testing.T) {
	d := newTestDriver(time.Minute)
	l := ledger.MustNewWithDefault("test-ledger")

	d.setCachedLedger(l)

	updated := l.WithMetadata(metadata.Metadata{"env": "prod"})
	d.setCachedLedger(updated)

	got, ok := d.getCachedLedger(l.Name)
	require.True(t, ok)
	require.Equal(t, "prod", got.Metadata["env"])
}

func TestCacheGenerationPreventsStaleWrites(t *testing.T) {
	d := newTestDriver(time.Minute)
	l := ledger.MustNewWithDefault("test-ledger")

	d.setCachedLedger(l)

	// Snapshot the generation before any eviction.
	d.mu.RLock()
	genBefore := d.cacheGens[l.Name]
	d.mu.RUnlock()

	d.evictCachedLedger(l.Name)

	// Generation must have been bumped.
	d.mu.RLock()
	genAfter := d.cacheGens[l.Name]
	d.mu.RUnlock()
	require.Greater(t, genAfter, genBefore, "eviction must increment the invalidation generation")

	// Simulate the singleflight write-back arriving with the stale generation.
	d.setCachedLedgerGen(l, genBefore)

	// The stale write must be silently rejected.
	_, ok := d.getCachedLedger(l.Name)
	require.False(t, ok, "write with stale generation must not populate the cache")
}

func TestSetCachedLedgerGenValidGeneration(t *testing.T) {
	d := newTestDriver(time.Minute)
	l := ledger.MustNewWithDefault("test-ledger")

	// Current generation is 0 (zero value); write with matching generation must succeed.
	d.setCachedLedgerGen(l, 0)

	got, ok := d.getCachedLedger(l.Name)
	require.True(t, ok, "write with current generation must populate the cache")
	require.Equal(t, l.Name, got.Name)
}

func TestSetCachedLedgerGenDisabledWhenTTLZero(t *testing.T) {
	d := newTestDriver(0)
	l := ledger.MustNewWithDefault("test-ledger")

	d.setCachedLedgerGen(l, 0)

	_, ok := d.getCachedLedger(l.Name)
	require.False(t, ok, "setCachedLedgerGen must be a no-op when TTL is zero")
}

func TestEvictCachedLedgersByBucketEvictsMatchingEntries(t *testing.T) {
	d := newTestDriver(time.Minute)

	la := ledger.MustNewWithDefault("ledger-a")
	la.Bucket = "target"
	lb := ledger.MustNewWithDefault("ledger-b")
	lb.Bucket = "target"

	d.setCachedLedger(la)
	d.setCachedLedger(lb)

	d.evictCachedLedgersByBucket("target")

	_, okA := d.getCachedLedger(la.Name)
	_, okB := d.getCachedLedger(lb.Name)
	require.False(t, okA, "ledger-a in target bucket must be evicted")
	require.False(t, okB, "ledger-b in target bucket must be evicted")
}

func TestEvictCachedLedgersByBucketPreservesOtherBuckets(t *testing.T) {
	d := newTestDriver(time.Minute)

	la := ledger.MustNewWithDefault("ledger-in-target")
	la.Bucket = "target"
	lo := ledger.MustNewWithDefault("ledger-in-other")
	lo.Bucket = "other"

	d.setCachedLedger(la)
	d.setCachedLedger(lo)

	d.evictCachedLedgersByBucket("target")

	_, okTarget := d.getCachedLedger(la.Name)
	require.False(t, okTarget)

	gotOther, okOther := d.getCachedLedger(lo.Name)
	require.True(t, okOther, "ledger in other bucket must be preserved")
	require.Equal(t, lo.Name, gotOther.Name)
}

func TestEvictCachedLedgersByBucketBumpsGenerations(t *testing.T) {
	d := newTestDriver(time.Minute)

	la := ledger.MustNewWithDefault("ledger-a")
	la.Bucket = "target"
	d.setCachedLedger(la)

	d.mu.RLock()
	genBefore := d.cacheGens[la.Name]
	d.mu.RUnlock()

	d.evictCachedLedgersByBucket("target")

	d.mu.RLock()
	genAfter := d.cacheGens[la.Name]
	d.mu.RUnlock()

	require.Greater(t, genAfter, genBefore, "bucket eviction must increment per-ledger generation")
}

func TestEvictCachedLedgersByBucketGenerationPreventsStaleWrites(t *testing.T) {
	d := newTestDriver(time.Minute)

	la := ledger.MustNewWithDefault("ledger-a")
	la.Bucket = "target"
	d.setCachedLedger(la)

	d.mu.RLock()
	genBefore := d.cacheGens[la.Name]
	d.mu.RUnlock()

	d.evictCachedLedgersByBucket("target")

	// Simulate a singleflight write-back that captured the old generation.
	d.setCachedLedgerGen(la, genBefore)

	_, ok := d.getCachedLedger(la.Name)
	require.False(t, ok, "stale-generation write after bucket eviction must be rejected")
}

func TestEvictCachedLedgersByBucketEmptyBucketIsNoOp(t *testing.T) {
	d := newTestDriver(time.Minute)

	l := ledger.MustNewWithDefault("ledger-other")
	l.Bucket = "other"
	d.setCachedLedger(l)

	require.NotPanics(t, func() {
		d.evictCachedLedgersByBucket("does-not-exist")
	})

	got, ok := d.getCachedLedger(l.Name)
	require.True(t, ok, "unrelated ledger must survive eviction of empty bucket")
	require.Equal(t, l.Name, got.Name)
}
