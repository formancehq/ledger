package kv

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMap_PutAndGet(t *testing.T) {
	t.Parallel()

	m := NewMap[string, int]()
	m.Put("a", 1)

	v, ok := m.Get("a")
	require.True(t, ok)
	require.Equal(t, 1, v)
}

func TestMap_GetMissing(t *testing.T) {
	t.Parallel()

	m := NewMap[string, int]()
	v, ok := m.Get("missing")
	require.False(t, ok)
	require.Equal(t, 0, v)
}

func TestMap_Del(t *testing.T) {
	t.Parallel()

	m := NewMap[string, int]()
	m.Put("a", 1)
	m.Del("a")

	_, ok := m.Get("a")
	require.False(t, ok)
}

func TestMap_Size(t *testing.T) {
	t.Parallel()

	m := NewMap[string, int]()
	require.Equal(t, uint64(0), m.Size())

	m.Put("a", 1)
	m.Put("b", 2)
	require.Equal(t, uint64(2), m.Size())

	m.Del("a")
	require.Equal(t, uint64(1), m.Size())
}

func TestMap_Iter(t *testing.T) {
	t.Parallel()

	m := NewMap[string, int]()
	m.Put("a", 1)
	m.Put("b", 2)
	m.Put("c", 3)

	collected := map[string]int{}
	for k, v := range m.Iter() {
		collected[k] = v
	}
	require.Equal(t, map[string]int{"a": 1, "b": 2, "c": 3}, collected)
}

func TestMap_PutOverwrite(t *testing.T) {
	t.Parallel()

	m := NewMap[string, int]()
	m.Put("a", 1)
	m.Put("a", 2)

	v, ok := m.Get("a")
	require.True(t, ok)
	require.Equal(t, 2, v)
	require.Equal(t, uint64(1), m.Size())
}

// --- SyncMap tests ---

func TestSyncMap_PutAndGet(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()
	m.Put("a", 1)

	v, ok := m.Get("a")
	require.True(t, ok)
	require.Equal(t, 1, v)
}

func TestSyncMap_GetMissing(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()
	v, ok := m.Get("missing")
	require.False(t, ok)
	require.Equal(t, 0, v)
}

func TestSyncMap_Del(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()
	m.Put("a", 1)
	m.Del("a")

	_, ok := m.Get("a")
	require.False(t, ok)
}

func TestSyncMap_DelMissing(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()
	// Deleting a non-existent key should not panic or change size
	m.Del("missing")
	require.Equal(t, uint64(0), m.Size())
}

func TestSyncMap_Size(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()
	require.Equal(t, uint64(0), m.Size())

	m.Put("a", 1)
	m.Put("b", 2)
	require.Equal(t, uint64(2), m.Size())

	m.Del("a")
	require.Equal(t, uint64(1), m.Size())
}

func TestSyncMap_PutOverwrite(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()
	m.Put("a", 1)
	m.Put("a", 2)

	v, ok := m.Get("a")
	require.True(t, ok)
	require.Equal(t, 2, v)
	require.Equal(t, uint64(1), m.Size())
}

func TestSyncMap_Iter(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()
	m.Put("a", 1)
	m.Put("b", 2)

	collected := map[string]int{}
	for k, v := range m.Iter() {
		collected[k] = v
	}
	require.Equal(t, map[string]int{"a": 1, "b": 2}, collected)
}

func TestSyncMap_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[int, int]()
	const goroutines = 100
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := range opsPerGoroutine {
				key := base*opsPerGoroutine + j
				m.Put(key, key)
				m.Get(key)
			}
		}(i)
	}
	wg.Wait()

	require.Equal(t, uint64(goroutines*opsPerGoroutine), m.Size())
}
