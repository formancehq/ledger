package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
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
			result := Gen(tt.index, tt.k)
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

	ac := cache.Volumes

	key := attributes.NewU128(1, 2)
	entry := attributes.Entry[*raftcmdpb.VolumePair]{
		Tag:  123,
		Data: &raftcmdpb.VolumePair{},
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

	ac := cache.Volumes

	key := attributes.NewU128(1, 2)
	entry := attributes.Entry[*raftcmdpb.VolumePair]{
		Tag:  123,
		Data: &raftcmdpb.VolumePair{},
	}

	ac.Put(key, entry)
	result, ok := ac.Get(key)
	require.True(t, ok)
	assert.False(t, result.Deleted)

	ac.Del(key)
	// Del marks as tombstone — the entry is still present but flagged as deleted.
	// KeyStore.Get filters tombstones, but AttributeCache.Get returns them
	// so that CheckCache/MirrorTouch still see the key as present.
	result, ok = ac.Get(key)
	assert.True(t, ok)
	assert.True(t, result.Deleted)
	// Data is reset to the zero value: a tombstone's payload is unreadable by
	// contract, and retaining it has historically caused snapshot/restore
	// foot-guns (EN-1377).
	assert.Nil(t, result.Data, "Del must reset entry.Data to the zero value")
}

func TestAttributeCache_DelResetsDataAcrossGenerations(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	ac := cache.Volumes

	key1 := attributes.NewU128(1, 1)
	key2 := attributes.NewU128(2, 2)

	ac.Put(key1, attributes.Entry[*raftcmdpb.VolumePair]{
		Tag:  10,
		Data: &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(100)},
	})

	ac.Rotate()

	// key1 is now in Gen1 (post-rotation). Add key2 to Gen0 with the same id
	// signature so both generations carry a payload — Del must clear both.
	ac.Put(key2, attributes.Entry[*raftcmdpb.VolumePair]{
		Tag:  20,
		Data: &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(200)},
	})

	ac.Del(key1)
	ac.Del(key2)

	got, ok := ac.Gen1().Get(key1)
	require.True(t, ok, "tombstone must remain visible in Gen1 for the cache predictor")
	assert.True(t, got.Deleted)
	assert.Nil(t, got.Data, "Gen1 tombstone payload must be reset")

	got, ok = ac.Gen0().Get(key2)
	require.True(t, ok, "tombstone must remain visible in Gen0 for the cache predictor")
	assert.True(t, got.Deleted)
	assert.Nil(t, got.Data, "Gen0 tombstone payload must be reset")
}

func TestAttributeCache_Size(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	ac := cache.Volumes

	assert.Equal(t, uint64(0), ac.Size())

	ac.Put(attributes.NewU128(1, 1), attributes.Entry[*raftcmdpb.VolumePair]{})
	assert.Equal(t, uint64(1), ac.Size())

	ac.Put(attributes.NewU128(2, 2), attributes.Entry[*raftcmdpb.VolumePair]{})
	assert.Equal(t, uint64(2), ac.Size())
}

func TestAttributeCache_Rotate(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	ac := cache.Volumes

	key1 := attributes.NewU128(1, 1)
	key2 := attributes.NewU128(2, 2)

	// Add to Gen0
	ac.Put(key1, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 1})

	// Rotate: Gen0 -> Gen1, new empty Gen0
	ac.Rotate()

	// key1 should still be accessible (now in Gen1)
	_, ok := ac.Get(key1)
	assert.True(t, ok, "key1 should be in Gen1 after rotation")

	// Add key2 to new Gen0
	ac.Put(key2, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 2})

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

	ac := cache.Volumes
	cache.SetCurrentGeneration(0)

	key := attributes.NewU128(1, 1)
	ac.Put(key, attributes.Entry[*raftcmdpb.VolumePair]{})

	// Index 5 is in generation 0 (same as current), key is in Gen0 -> true
	assert.True(t, ac.IsGuaranteedInCache(5, key))

	// Non-existent key
	nonExistent := attributes.NewU128(99, 99)
	assert.False(t, ac.IsGuaranteedInCache(5, nonExistent))

	// Key in Gen1 only needs a touch — not guaranteed (would be lost after next rotation)
	gen1Key := attributes.NewU128(3, 3)
	ac.Gen1().Put(gen1Key, attributes.Entry[*raftcmdpb.VolumePair]{})
	assert.False(t, ac.IsGuaranteedInCache(5, gen1Key))
	assert.Equal(t, CacheNeedsTouch, ac.CheckCache(5, gen1Key))
}

func TestAttributeCache_IsGuaranteedInCache_NextGeneration(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	ac := cache.Volumes
	cache.SetCurrentGeneration(0)

	keyInGen0 := attributes.NewU128(1, 1)
	keyInGen1 := attributes.NewU128(2, 2)

	// Put keyInGen0 in Gen0
	ac.Put(keyInGen0, attributes.Entry[*raftcmdpb.VolumePair]{})

	// Simulate keyInGen1 being in Gen1 only
	ac.Gen1().Put(keyInGen1, attributes.Entry[*raftcmdpb.VolumePair]{})

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

	ac := cache.Volumes
	cache.SetCurrentGeneration(0)

	key := attributes.NewU128(1, 1)
	ac.Put(key, attributes.Entry[*raftcmdpb.VolumePair]{})

	// Index 25 is in generation 2 (two generations ahead): any preload
	// computed now would be rotated out before apply. CheckCache surfaces
	// this as CacheUnreachable so admission can reject the proposal with a
	// transient error — Guaranteed must be false and the explicit status
	// must be CacheUnreachable (not CacheMiss, which would mistakenly drive
	// a stale preload).
	assert.False(t, ac.IsGuaranteedInCache(25, key))
	assert.Equal(t, CacheUnreachable, ac.CheckCache(25, key),
		"≥2 generations ahead must report CacheUnreachable")

	// Same regime for a key absent from the cache: still Unreachable; the
	// admission-level reject takes precedence over the per-key miss path.
	absent := attributes.NewU128(9, 9)
	assert.Equal(t, CacheUnreachable, ac.CheckCache(25, absent))
}

func TestCache_NewCache(t *testing.T) {
	t.Parallel()

	cache, err := New(100, nil)
	require.NoError(t, err)

	assert.NotNil(t, cache.Volumes)
	assert.NotNil(t, cache.AccountMetadata)
	assert.Equal(t, uint64(100), cache.GenerationThreshold())
	assert.Equal(t, uint64(0), cache.CurrentGeneration())
	assert.Equal(t, uint64(0), cache.BaseIndex.Gen0)
	assert.Equal(t, uint64(0), cache.BaseIndex.Gen1)
	// Epoch must start at 1, not 0 — the FSM staleness check used to treat
	// epoch 0 as "unset" and skip the comparison, leaving the check inert
	// for the entire first epoch of a cluster's life (#302).
	assert.Equal(t, uint64(1), cache.Epoch(), "fresh cache must report epoch=1, never 0")
}

func TestCache_ResetWithThresholdIncrementsEpoch(t *testing.T) {
	t.Parallel()

	cache, err := New(100, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), cache.Epoch())

	cache.ResetWithThreshold(200)
	assert.Equal(t, uint64(2), cache.Epoch())

	cache.ResetWithThreshold(300)
	assert.Equal(t, uint64(3), cache.Epoch())
}

func TestCache_CheckRotationNeeded_SameGeneration(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)
	cache.SetCurrentGeneration(0)

	// Add some data
	key := attributes.NewU128(1, 1)
	cache.Volumes.Put(key, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 1})

	// Check at index 5 (still generation 0)
	cache.CheckRotationNeeded(5)
	assert.Equal(t, uint64(0), cache.CurrentGeneration())

	// Data should still be in Gen0
	_, ok := cache.Volumes.Get(key)
	assert.True(t, ok)
}

func TestCache_CheckRotationNeeded_NewGeneration(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)
	cache.SetCurrentGeneration(0)

	// Add some data
	key := attributes.NewU128(1, 1)
	cache.Volumes.Put(key, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 1})

	// Check at index 11 (generation 1)
	cache.CheckRotationNeeded(11)
	assert.Equal(t, uint64(1), cache.CurrentGeneration())

	// Data should now be in Gen1 (still accessible)
	_, ok := cache.Volumes.Get(key)
	assert.True(t, ok)

	// BaseIndex should be the canonical boundary genEndIndex(0, 10) = 10
	assert.Equal(t, uint64(10), cache.BaseIndex.Gen0)
}

func TestCache_CheckRotationNeeded_MultipleGenerations(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)
	cache.SetCurrentGeneration(0)

	keyGen0 := attributes.NewU128(1, 1)
	cache.Volumes.Put(keyGen0, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 1})

	// Move to generation 1
	cache.CheckRotationNeeded(11)
	assert.Equal(t, uint64(1), cache.CurrentGeneration())

	keyGen1 := attributes.NewU128(2, 2)
	cache.Volumes.Put(keyGen1, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 2})

	// Both keys accessible
	_, ok := cache.Volumes.Get(keyGen0)
	assert.True(t, ok)
	_, ok = cache.Volumes.Get(keyGen1)
	assert.True(t, ok)

	// Move to generation 2
	cache.CheckRotationNeeded(21)
	assert.Equal(t, uint64(2), cache.CurrentGeneration())

	// keyGen0 should be gone, keyGen1 should still be there
	_, ok = cache.Volumes.Get(keyGen0)
	assert.False(t, ok, "keyGen0 should be gone after second rotation")
	_, ok = cache.Volumes.Get(keyGen1)
	assert.True(t, ok, "keyGen1 should still be accessible")
}

func TestCache_NewCache_ZeroThresholdReturnsError(t *testing.T) {
	t.Parallel()

	_, err := New(0, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "generation threshold must be greater than zero")
}

func TestAttributeCache_Reset(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	key := attributes.NewU128(1, 1)
	cache.Volumes.Put(key, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 42})

	_, ok := cache.Volumes.Get(key)
	require.True(t, ok)

	cache.Volumes.Reset()

	_, ok = cache.Volumes.Get(key)
	assert.False(t, ok, "data should be gone after reset")
	assert.Equal(t, uint64(0), cache.Volumes.Size())
}

func TestCache_Reset(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	// Add data and advance generation
	key := attributes.NewU128(1, 1)
	cache.Volumes.Put(key, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 1})
	cache.AccountMetadata.Put(key, attributes.Entry[*commonpb.MetadataValue]{Tag: 2})
	cache.CheckRotationNeeded(11)
	require.Equal(t, uint64(1), cache.CurrentGeneration())

	cache.Reset()

	assert.Equal(t, uint64(0), cache.CurrentGeneration())
	assert.Equal(t, uint64(0), cache.Volumes.Size())
	assert.Equal(t, uint64(0), cache.AccountMetadata.Size())
	assert.Equal(t, uint64(0), cache.BaseIndex.Gen0)
	assert.Equal(t, uint64(0), cache.BaseIndex.Gen1)
}

func TestAttributeCache_Iter(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	ac := cache.Volumes

	// Add data to gen0
	key1 := attributes.NewU128(1, 1)
	key2 := attributes.NewU128(2, 2)

	ac.Put(key1, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 10})
	ac.Put(key2, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 20})

	// Rotate so key1/key2 are in gen1
	ac.Rotate()

	// Add key3 to gen0
	key3 := attributes.NewU128(3, 3)
	ac.Put(key3, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 30})

	// Iter should yield all 3 entries
	seen := make(map[uint64]bool)
	for _, entry := range ac.Iter() {
		seen[entry.Tag] = true
	}

	assert.True(t, seen[10])
	assert.True(t, seen[20])
	assert.True(t, seen[30])
}

func TestAttributeCache_Gen0Gen1_Accessors(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	ac := cache.Volumes

	require.NotNil(t, ac.Gen0())
	require.NotNil(t, ac.Gen1())

	// Gen0 and Gen1 should be different instances
	require.NotSame(t, ac.Gen0(), ac.Gen1())
}

func TestCache_CheckRotationNeeded_ZeroThreshold(t *testing.T) {
	t.Parallel()

	// Create a cache and forcibly set threshold to 0
	cache, err := New(10, nil)
	require.NoError(t, err)

	cache.SetGenerationThreshold(0)

	rotated, _ := cache.CheckRotationNeeded(100)
	assert.False(t, rotated)
}

func TestAttributeCache_IsGuaranteedInCache_ZeroThreshold(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	cache.SetGenerationThreshold(0)

	key := attributes.NewU128(1, 1)
	cache.Volumes.Put(key, attributes.Entry[*raftcmdpb.VolumePair]{})

	// Should return false when threshold is 0
	assert.False(t, cache.Volumes.IsGuaranteedInCache(5, key))
}

func TestAttributeCache_Touch_DoesNotOverwriteGen0(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	ac := cache.Volumes

	key := attributes.NewU128(1, 1)

	// Put a value in Gen0, then rotate so it moves to Gen1
	gen1Entry := attributes.Entry[*raftcmdpb.VolumePair]{Tag: 100}
	ac.Put(key, gen1Entry)
	ac.Rotate()

	// Touch promotes Gen1 → Gen0 (Gen0 is empty)
	ac.Touch(key)
	v, ok := ac.Gen0().Get(key)
	require.True(t, ok)
	assert.Equal(t, uint64(100), v.Tag)

	// Simulate a Merge updating Gen0 with a newer value
	gen0Entry := attributes.Entry[*raftcmdpb.VolumePair]{Tag: 200}
	ac.Gen0().Put(key, gen0Entry)

	// A redundant Touch (from a concurrent admission) must NOT overwrite the newer Gen0 value
	ac.Touch(key)
	v, ok = ac.Gen0().Get(key)
	require.True(t, ok)
	assert.Equal(t, uint64(200), v.Tag, "Touch must not overwrite existing Gen0 entry")
}

func TestCache_AllAttributeCachesRotate(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	// Add data to all caches
	key := attributes.NewU128(1, 1)
	cache.Volumes.Put(key, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 1})
	cache.AccountMetadata.Put(key, attributes.Entry[*commonpb.MetadataValue]{Tag: 2})

	// Trigger rotation
	cache.CheckRotationNeeded(11)

	// All data should still be accessible (in Gen1)
	_, ok := cache.Volumes.Get(key)
	assert.True(t, ok)
	_, ok = cache.AccountMetadata.Get(key)
	assert.True(t, ok)

	// Trigger another rotation
	cache.CheckRotationNeeded(21)

	// All data should be gone
	_, ok = cache.Volumes.Get(key)
	assert.False(t, ok)
	_, ok = cache.AccountMetadata.Get(key)
	assert.False(t, ok)
}
