package kv

import (
	"hash/fnv"
	"maps"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func stringHash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))

	return h.Sum64()
}

func intHash(i int) uint64 {
	return uint64(i)
}

func TestShardedMap_PutAndGet(t *testing.T) {
	t.Parallel()

	sm := NewShardedMap[string, int](stringHash)
	sm.Put("a", 1)

	v, ok := sm.Get("a")
	require.True(t, ok)
	require.Equal(t, 1, v)
}

func TestShardedMap_GetMissing(t *testing.T) {
	t.Parallel()

	sm := NewShardedMap[string, int](stringHash)
	_, ok := sm.Get("missing")
	require.False(t, ok)
}

func TestShardedMap_Del(t *testing.T) {
	t.Parallel()

	sm := NewShardedMap[string, int](stringHash)
	sm.Put("a", 1)
	sm.Del("a")

	_, ok := sm.Get("a")
	require.False(t, ok)
}

func TestShardedMap_DelMissing(t *testing.T) {
	t.Parallel()

	sm := NewShardedMap[string, int](stringHash)
	sm.Del("missing")
	require.Equal(t, uint64(0), sm.Size())
}

func TestShardedMap_Size(t *testing.T) {
	t.Parallel()

	sm := NewShardedMap[string, int](stringHash)
	require.Equal(t, uint64(0), sm.Size())

	sm.Put("a", 1)
	sm.Put("b", 2)
	sm.Put("c", 3)
	require.Equal(t, uint64(3), sm.Size())

	sm.Del("b")
	require.Equal(t, uint64(2), sm.Size())
}

func TestShardedMap_PutOverwrite(t *testing.T) {
	t.Parallel()

	sm := NewShardedMap[string, int](stringHash)
	sm.Put("a", 1)
	sm.Put("a", 2)

	v, ok := sm.Get("a")
	require.True(t, ok)
	require.Equal(t, 2, v)
	require.Equal(t, uint64(1), sm.Size())
}

func TestShardedMap_Iter(t *testing.T) {
	t.Parallel()

	sm := NewShardedMap[string, int](stringHash)
	sm.Put("a", 1)
	sm.Put("b", 2)
	sm.Put("c", 3)

	collected := maps.Collect(sm.Iter())

	require.Equal(t, map[string]int{"a": 1, "b": 2, "c": 3}, collected)
}

func TestShardedMap_ShardDistribution(t *testing.T) {
	t.Parallel()

	sm := NewShardedMap[int, bool](intHash)
	// Insert keys that map to different shards
	for i := range 128 {
		sm.Put(i, true)
	}

	require.Equal(t, uint64(128), sm.Size())
}

func TestShardedMap_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	sm := NewShardedMap[int, int](intHash)

	const (
		goroutines      = 50
		opsPerGoroutine = 200
	)

	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)

		go func(base int) {
			defer wg.Done()

			for j := range opsPerGoroutine {
				key := base*opsPerGoroutine + j
				sm.Put(key, key)

				v, ok := sm.Get(key)
				if ok && v != key {
					t.Errorf("expected %d, got %d", key, v)
				}
			}
		}(i)
	}

	wg.Wait()

	require.Equal(t, uint64(goroutines*opsPerGoroutine), sm.Size())
}

func TestShardedMap_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()

	sm := NewShardedMap[int, int](intHash)

	const n = 1000

	// Pre-populate
	for i := range n {
		sm.Put(i, i)
	}

	var wg sync.WaitGroup

	// Concurrent readers
	for range 10 {
		wg.Go(func() {
			for i := range n {
				sm.Get(i)
			}
		})
	}

	// Concurrent writers
	for range 5 {
		wg.Go(func() {
			for i := range n {
				sm.Put(i, i*2)
			}
		})
	}

	wg.Wait()
	require.Equal(t, uint64(n), sm.Size())
}
