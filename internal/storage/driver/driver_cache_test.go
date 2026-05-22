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
	d := newTestDriver(time.Nanosecond)
	l := ledger.MustNewWithDefault("test-ledger")

	d.setCachedLedger(l)
	time.Sleep(2 * time.Nanosecond)

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
