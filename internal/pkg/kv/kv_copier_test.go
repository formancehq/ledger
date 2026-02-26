package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func identity[V any](v V) V { return v }

func TestCopier_GetFromSource(t *testing.T) {
	t.Parallel()

	src := NewMap[string, int]()
	src.Put("a", 1)

	c := NewCopier(identity[int], src)

	v, ok := c.Get("a")
	require.True(t, ok)
	require.Equal(t, 1, v)
}

func TestCopier_GetMissing(t *testing.T) {
	t.Parallel()

	src := NewMap[string, int]()
	c := NewCopier(identity[int], src)

	_, ok := c.Get("missing")
	require.False(t, ok)
}

func TestCopier_PutOverridesSource(t *testing.T) {
	t.Parallel()

	src := NewMap[string, int]()
	src.Put("a", 1)

	c := NewCopier(identity[int], src)
	c.Put("a", 99)

	v, ok := c.Get("a")
	require.True(t, ok)
	require.Equal(t, 99, v)
}

func TestCopier_Del(t *testing.T) {
	t.Parallel()

	src := NewMap[string, int]()
	src.Put("a", 5)

	c := NewCopier(identity[int], src)
	c.Del("a")

	v, ok := c.Get("a")
	require.True(t, ok)
	require.Equal(t, 0, v) // Del puts zero value
}

func TestCopier_LoadOrInit_Existing(t *testing.T) {
	t.Parallel()

	src := NewMap[string, int]()
	src.Put("a", 42)

	c := NewCopier(identity[int], src)
	v := c.LoadOrInit("a", func() int { return 999 })
	require.Equal(t, 42, v)
}

func TestCopier_LoadOrInit_New(t *testing.T) {
	t.Parallel()

	src := NewMap[string, int]()
	c := NewCopier(identity[int], src)

	v := c.LoadOrInit("new", func() int { return 999 })
	require.Equal(t, 999, v)
}

func TestCopier_Merge(t *testing.T) {
	t.Parallel()

	src := NewMap[string, int]()
	src.Put("a", 1)

	c := NewCopier(identity[int], src)
	c.Put("b", 2)
	c.Put("a", 10)

	merged := c.Merge()

	v, ok := merged.Get("a")
	require.True(t, ok)
	require.Equal(t, 10, v)

	v, ok = merged.Get("b")
	require.True(t, ok)
	require.Equal(t, 2, v)
}

func TestCopier_MergeIdempotent(t *testing.T) {
	t.Parallel()

	src := NewMap[string, int]()
	c := NewCopier(identity[int], src)
	c.Put("a", 1)

	first := c.Merge()
	second := c.Merge()
	require.Equal(t, first, second)
}

func TestCopier_PutAfterMergePanics(t *testing.T) {
	t.Parallel()

	src := NewMap[string, int]()
	c := NewCopier(identity[int], src)
	c.Merge()

	require.Panics(t, func() {
		c.Put("a", 1)
	})
}

func TestCopier_Updates(t *testing.T) {
	t.Parallel()

	src := NewMap[string, int]()
	src.Put("existing", 10)

	c := NewCopier(identity[int], src)

	// Read from source to trigger copy
	_, _ = c.Get("existing")
	c.Put("existing", 20)
	c.Put("new", 30)

	updates := c.Updates()
	require.Len(t, updates, 2)

	byKey := map[string]CopierUpdate[string, int]{}
	for _, u := range updates {
		byKey[u.Key] = u
	}

	existingUpdate := byKey["existing"]
	require.True(t, existingUpdate.Old.IsDefined())
	require.Equal(t, 10, existingUpdate.Old.Value())
	require.Equal(t, 20, existingUpdate.New)

	newUpdate := byKey["new"]
	require.False(t, newUpdate.Old.IsDefined())
	require.Equal(t, 30, newUpdate.New)
}

func TestCopier_CopyOnRead(t *testing.T) {
	t.Parallel()

	type box struct{ val int }

	src := NewMap[string, *box]()
	original := &box{val: 1}
	src.Put("key", original)

	copyFunc := func(b *box) *box { return &box{val: b.val} }
	c := NewCopier(copyFunc, src)

	got, ok := c.Get("key")
	require.True(t, ok)
	require.Equal(t, 1, got.val)

	// Mutating the copy should not affect the source
	got.val = 999
	require.Equal(t, 1, original.val)
}

func TestCopier_MultipleSources(t *testing.T) {
	t.Parallel()

	src1 := NewMap[string, int]()
	src1.Put("a", 1)

	src2 := NewMap[string, int]()
	src2.Put("b", 2)

	c := NewCopier(identity[int], src1, src2)

	v, ok := c.Get("a")
	require.True(t, ok)
	require.Equal(t, 1, v)

	v, ok = c.Get("b")
	require.True(t, ok)
	require.Equal(t, 2, v)
}

func TestCopier_SizeAndIter(t *testing.T) {
	t.Parallel()

	src := NewMap[string, int]()
	c := NewCopier(identity[int], src)

	c.Put("a", 1)
	c.Put("b", 2)

	require.Equal(t, uint64(2), c.Size())

	collected := map[string]int{}
	for k, v := range c.Iter() {
		collected[k] = v
	}
	require.Equal(t, map[string]int{"a": 1, "b": 2}, collected)
}
