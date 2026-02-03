package cache

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		index    uint64
		k        uint64
		expected uint64
	}{
		{"index 0 returns 0", 0, 10, 0},
		{"index 1 with k=10 returns 0", 1, 10, 0},
		{"index 10 with k=10 returns 0", 10, 10, 0},
		{"index 11 with k=10 returns 1", 11, 10, 1},
		{"index 20 with k=10 returns 1", 20, 10, 1},
		{"index 21 with k=10 returns 2", 21, 10, 2},
		{"index 100 with k=10 returns 9", 100, 10, 9},
		{"index 101 with k=10 returns 10", 101, 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gen(tt.index, tt.k)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenStartIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		g        uint64
		k        uint64
		expected uint64
	}{
		{"gen 0 with k=10 starts at 1", 0, 10, 1},
		{"gen 1 with k=10 starts at 11", 1, 10, 11},
		{"gen 2 with k=10 starts at 21", 2, 10, 21},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := genStartIndex(tt.g, tt.k)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenEndIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		g        uint64
		k        uint64
		expected uint64
	}{
		{"gen 0 with k=10 ends at 10", 0, 10, 10},
		{"gen 1 with k=10 ends at 20", 1, 10, 20},
		{"gen 2 with k=10 ends at 30", 2, 10, 30},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := genEndIndex(tt.g, tt.k)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBoundaryIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		index    uint64
		k        uint64
		expected uint64
	}{
		{"index in gen 0 returns 0", 5, 10, 0},
		{"index in gen 1 returns 10", 15, 10, 10},
		{"index in gen 2 returns 20", 25, 10, 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BoundaryIndex(tt.index, tt.k)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDualGen_Rotate(t *testing.T) {
	t.Parallel()

	d := newDualGen[int](1, 0)
	assert.Equal(t, 1, d.Gen0)
	assert.Equal(t, 0, d.Gen1)

	// First rotation
	old := d.Rotate(2)
	assert.Equal(t, 1, old.Gen0, "old Gen0 should be preserved")
	assert.Equal(t, 0, old.Gen1, "old Gen1 should be preserved")
	assert.Equal(t, 2, d.Gen0, "new Gen0 should be the new value")
	assert.Equal(t, 1, d.Gen1, "new Gen1 should be old Gen0")

	// Second rotation
	d.Rotate(3)
	assert.Equal(t, 3, d.Gen0)
	assert.Equal(t, 2, d.Gen1)
}

func TestDualGen_Update(t *testing.T) {
	t.Parallel()

	type counter struct{ value int }
	d := newDualGen[*counter](&counter{value: 0}, &counter{value: 0})

	d.Update(func(c *counter) {
		c.value = 42
	})

	assert.Equal(t, 42, d.Gen0.value)
	assert.Equal(t, 0, d.Gen1.value)
}

func TestAttributeCache_PutGet(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)
	ac := cache.Input

	key := attributes.NewU128(1, 2)
	entry := attributes.Entry[*raftcmdpb.VolumeHolder]{
		Tag:  123,
		Data: &raftcmdpb.VolumeHolder{},
	}

	// Get on empty cache
	_, ok := ac.Get(key)
	assert.False(t, ok)

	// Put and Get
	ac.Put(key, entry)
	result, ok := ac.Get(key)
	require.True(t, ok)
	assert.Equal(t, uint64(123), result.Tag)
}

func TestAttributeCache_Del(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)
	ac := cache.Input

	key := attributes.NewU128(1, 2)
	entry := attributes.Entry[*raftcmdpb.VolumeHolder]{
		Tag:  123,
		Data: &raftcmdpb.VolumeHolder{},
	}

	ac.Put(key, entry)
	_, ok := ac.Get(key)
	require.True(t, ok)

	ac.Del(key)
	_, ok = ac.Get(key)
	assert.False(t, ok)
}

func TestAttributeCache_Size(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)
	ac := cache.Input

	assert.Equal(t, uint64(0), ac.Size())

	ac.Put(attributes.NewU128(1, 1), attributes.Entry[*raftcmdpb.VolumeHolder]{})
	assert.Equal(t, uint64(1), ac.Size())

	ac.Put(attributes.NewU128(2, 2), attributes.Entry[*raftcmdpb.VolumeHolder]{})
	assert.Equal(t, uint64(2), ac.Size())
}

func TestAttributeCache_Rotate(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)
	ac := cache.Input

	key1 := attributes.NewU128(1, 1)
	key2 := attributes.NewU128(2, 2)

	// Add to Gen0
	ac.Put(key1, attributes.Entry[*raftcmdpb.VolumeHolder]{Tag: 1})

	// Rotate: Gen0 -> Gen1, new empty Gen0
	ac.Rotate()

	// key1 should still be accessible (now in Gen1)
	_, ok := ac.Get(key1)
	assert.True(t, ok, "key1 should be in Gen1 after rotation")

	// Add key2 to new Gen0
	ac.Put(key2, attributes.Entry[*raftcmdpb.VolumeHolder]{Tag: 2})

	// Both keys should be accessible
	_, ok = ac.Get(key1)
	assert.True(t, ok)
	_, ok = ac.Get(key2)
	assert.True(t, ok)

	// Rotate again: Gen1 (with key1) is discarded
	ac.Rotate()

	// key1 should be gone, key2 should be in Gen1
	_, ok = ac.Get(key1)
	assert.False(t, ok, "key1 should be gone after second rotation")
	_, ok = ac.Get(key2)
	assert.True(t, ok, "key2 should be in Gen1 after second rotation")
}

func TestAttributeCache_IsGuaranteedInCache_SameGeneration(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)
	ac := cache.Input
	cache.CurrentGeneration = 0

	key := attributes.NewU128(1, 1)
	ac.Put(key, attributes.Entry[*raftcmdpb.VolumeHolder]{})

	// Index 5 is in generation 0 (same as current), data will be there
	assert.True(t, ac.IsGuaranteedInCache(5, key))

	// Non-existent key
	nonExistent := attributes.NewU128(99, 99)
	assert.False(t, ac.IsGuaranteedInCache(5, nonExistent))
}

func TestAttributeCache_IsGuaranteedInCache_NextGeneration(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)
	ac := cache.Input
	cache.CurrentGeneration = 0

	keyInGen0 := attributes.NewU128(1, 1)
	keyInGen1 := attributes.NewU128(2, 2)

	// Put keyInGen0 in Gen0
	ac.Put(keyInGen0, attributes.Entry[*raftcmdpb.VolumeHolder]{})

	// Simulate keyInGen1 being in Gen1 only
	ac.mu.Lock()
	ac.Gen1.Put(keyInGen1, attributes.Entry[*raftcmdpb.VolumeHolder]{})
	ac.mu.Unlock()

	// Index 15 is in generation 1 (next generation)
	// keyInGen0 is in Gen0, will survive one rotation -> true
	assert.True(t, ac.IsGuaranteedInCache(15, keyInGen0))

	// keyInGen1 is in Gen1, will be discarded after rotation -> false
	assert.False(t, ac.IsGuaranteedInCache(15, keyInGen1))
}

func TestAttributeCache_IsGuaranteedInCache_TwoGenerationsAhead(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)
	ac := cache.Input
	cache.CurrentGeneration = 0

	key := attributes.NewU128(1, 1)
	ac.Put(key, attributes.Entry[*raftcmdpb.VolumeHolder]{})

	// Index 25 is in generation 2 (two generations ahead)
	// Data will be lost after two rotations -> false
	assert.False(t, ac.IsGuaranteedInCache(25, key))
}

func TestCache_NewCache(t *testing.T) {
	t.Parallel()

	cache, err := New(100, nil)
	require.NoError(t, err)

	assert.NotNil(t, cache.Input)
	assert.NotNil(t, cache.Output)
	assert.NotNil(t, cache.AccountMetadata)
	assert.Equal(t, uint64(100), cache.GenerationThreshold)
	assert.Equal(t, uint64(0), cache.CurrentGeneration)
	assert.Equal(t, uint64(1), cache.BaseIndex.Gen0)
	assert.Equal(t, uint64(0), cache.BaseIndex.Gen1)
}

func TestCache_CheckRotationNeeded_SameGeneration(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)
	cache.CurrentGeneration = 0

	// Add some data
	key := attributes.NewU128(1, 1)
	cache.Input.Put(key, attributes.Entry[*raftcmdpb.VolumeHolder]{Tag: 1})

	// Check at index 5 (still generation 0)
	cache.CheckRotationNeeded(5)
	assert.Equal(t, uint64(0), cache.CurrentGeneration)

	// Data should still be in Gen0
	_, ok := cache.Input.Get(key)
	assert.True(t, ok)
}

func TestCache_CheckRotationNeeded_NewGeneration(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)
	cache.CurrentGeneration = 0

	// Add some data
	key := attributes.NewU128(1, 1)
	cache.Input.Put(key, attributes.Entry[*raftcmdpb.VolumeHolder]{Tag: 1})

	// Check at index 11 (generation 1)
	cache.CheckRotationNeeded(11)
	assert.Equal(t, uint64(1), cache.CurrentGeneration)

	// Data should now be in Gen1 (still accessible)
	_, ok := cache.Input.Get(key)
	assert.True(t, ok)

	// BaseIndex should be updated
	assert.Equal(t, uint64(11), cache.BaseIndex.Gen0)
}

func TestCache_CheckRotationNeeded_MultipleGenerations(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)
	cache.CurrentGeneration = 0

	keyGen0 := attributes.NewU128(1, 1)
	cache.Input.Put(keyGen0, attributes.Entry[*raftcmdpb.VolumeHolder]{Tag: 1})

	// Move to generation 1
	cache.CheckRotationNeeded(11)
	assert.Equal(t, uint64(1), cache.CurrentGeneration)

	keyGen1 := attributes.NewU128(2, 2)
	cache.Input.Put(keyGen1, attributes.Entry[*raftcmdpb.VolumeHolder]{Tag: 2})

	// Both keys accessible
	_, ok := cache.Input.Get(keyGen0)
	assert.True(t, ok)
	_, ok = cache.Input.Get(keyGen1)
	assert.True(t, ok)

	// Move to generation 2
	cache.CheckRotationNeeded(21)
	assert.Equal(t, uint64(2), cache.CurrentGeneration)

	// keyGen0 should be gone, keyGen1 should still be there
	_, ok = cache.Input.Get(keyGen0)
	assert.False(t, ok, "keyGen0 should be gone after second rotation")
	_, ok = cache.Input.Get(keyGen1)
	assert.True(t, ok, "keyGen1 should still be accessible")
}

func TestCache_AllAttributeCachesRotate(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	// Add data to all caches
	key := attributes.NewU128(1, 1)
	cache.Input.Put(key, attributes.Entry[*raftcmdpb.VolumeHolder]{Tag: 1})
	cache.Output.Put(key, attributes.Entry[*raftcmdpb.VolumeHolder]{Tag: 2})
	cache.AccountMetadata.Put(key, attributes.Entry[*commonpb.MetadataValue]{Tag: 3})

	// Trigger rotation
	cache.CheckRotationNeeded(11)

	// All data should still be accessible (in Gen1)
	_, ok := cache.Input.Get(key)
	assert.True(t, ok)
	_, ok = cache.Output.Get(key)
	assert.True(t, ok)
	_, ok = cache.AccountMetadata.Get(key)
	assert.True(t, ok)

	// Trigger another rotation
	cache.CheckRotationNeeded(21)

	// All data should be gone
	_, ok = cache.Input.Get(key)
	assert.False(t, ok)
	_, ok = cache.Output.Get(key)
	assert.False(t, ok)
	_, ok = cache.AccountMetadata.Get(key)
	assert.False(t, ok)
}
