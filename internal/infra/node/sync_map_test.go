package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewSyncMap(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()
	require.NotNil(t, m)

	// New map should be empty
	count := 0
	m.Range(func(_ string, _ int) bool {
		count++

		return true
	})

	require.Zero(t, count)
}

func TestSyncMap_StoreAndLoad(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()

	m.Store("key1", 42)

	val, ok := m.Load("key1")
	require.True(t, ok)
	require.Equal(t, 42, val)
}

func TestSyncMap_LoadMissing(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()

	val, ok := m.Load("nonexistent")
	require.False(t, ok)
	require.Zero(t, val)
}

func TestSyncMap_LoadOrStore_New(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()

	actual, loaded := m.LoadOrStore("key1", 42)
	require.False(t, loaded)
	require.Equal(t, 42, actual)

	// Verify the value was stored
	val, ok := m.Load("key1")
	require.True(t, ok)
	require.Equal(t, 42, val)
}

func TestSyncMap_LoadOrStore_Existing(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()

	m.Store("key1", 42)

	actual, loaded := m.LoadOrStore("key1", 99)
	require.True(t, loaded)
	require.Equal(t, 42, actual)

	// Value should not have changed
	val, ok := m.Load("key1")
	require.True(t, ok)
	require.Equal(t, 42, val)
}

func TestSyncMap_Delete(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()

	m.Store("key1", 42)
	m.Delete("key1")

	_, ok := m.Load("key1")
	require.False(t, ok)
}

func TestSyncMap_LoadAndDelete(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()

	m.Store("key1", 42)

	val, loaded := m.LoadAndDelete("key1")
	require.True(t, loaded)
	require.Equal(t, 42, val)

	// Verify it was deleted
	_, ok := m.Load("key1")
	require.False(t, ok)
}

func TestSyncMap_LoadAndDelete_Missing(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()

	val, loaded := m.LoadAndDelete("nonexistent")
	require.False(t, loaded)
	require.Zero(t, val)
}

func TestSyncMap_Range(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[string, int]()

	m.Store("a", 1)
	m.Store("b", 2)
	m.Store("c", 3)

	collected := make(map[string]int)
	m.Range(func(k string, v int) bool {
		collected[k] = v

		return true
	})

	require.Len(t, collected, 3)
	require.Equal(t, 1, collected["a"])
	require.Equal(t, 2, collected["b"])
	require.Equal(t, 3, collected["c"])
}

func TestSyncMap_Range_EarlyStop(t *testing.T) {
	t.Parallel()

	m := NewSyncMap[int, string]()

	m.Store(1, "a")
	m.Store(2, "b")
	m.Store(3, "c")

	count := 0
	m.Range(func(_ int, _ string) bool {
		count++

		return false // stop after first
	})

	require.Equal(t, 1, count)
}
