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

	// Rotate: Gen0 -> Gen1, new empty Gen0.
	ac.Rotate()

	// Get falls back to Gen1 when Gen0 misses.
	_, ok := ac.Get(key1)
	assert.True(t, ok, "key1 must be reachable via Get's Gen1 fallback after rotation")

	// Add key2 to new Gen0.
	ac.Put(key2, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 2})

	// Both keys visible via Get.
	_, ok = ac.Get(key1)
	assert.True(t, ok)
	_, ok = ac.Get(key2)
	assert.True(t, ok)

	// Rotate again: Gen1 (with key1) is discarded.
	ac.Rotate()
	_, ok = ac.Get(key1)
	assert.False(t, ok, "key1 fully evicted after two rotations")
	_, ok = ac.Get(key2)
	assert.True(t, ok, "key2 sits in Gen1 after the second rotation")
}

func TestAttributeCache_CheckCache_SameGeneration(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	ac := cache.Volumes
	cache.SetCurrentGeneration(0)

	// Gen0 hit → CacheHit.
	gen0Key := attributes.NewU128(1, 1)
	ac.Put(gen0Key, attributes.Entry[*raftcmdpb.VolumePair]{})
	assert.Equal(t, CacheHit, ac.CheckCache(5, gen0Key))

	// Gen1-only entry within the same-generation horizon: the FSM apply
	// path reads via AttributeCache.Get which falls back to Gen1, so
	// admission does not need to distinguish "already Gen0" from
	// "Gen1-only" — both return CacheHit.
	gen1Key := attributes.NewU128(3, 3)
	ac.Gen1().Put(gen1Key, attributes.Entry[*raftcmdpb.VolumePair]{})
	assert.Equal(t, CacheHit, ac.CheckCache(5, gen1Key))

	// Absent from both generations → CacheMiss.
	nonExistent := attributes.NewU128(99, 99)
	assert.Equal(t, CacheMiss, ac.CheckCache(5, nonExistent))
}

func TestAttributeCache_CheckCache_NextGeneration(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	ac := cache.Volumes
	cache.SetCurrentGeneration(0)

	keyInGen0 := attributes.NewU128(1, 1)
	keyInGen1 := attributes.NewU128(2, 2)

	// Put keyInGen0 in Gen0 (rotation will move it to new Gen1 by apply
	// time; Get's gen0→gen1 fallback still surfaces it, and lazy Del
	// promotes it if the handler tombstones).
	ac.Put(keyInGen0, attributes.Entry[*raftcmdpb.VolumePair]{})

	// Simulate keyInGen1 being in Gen1 only (rotation will discard it —
	// no promote can save it).
	ac.Gen1().Put(keyInGen1, attributes.Entry[*raftcmdpb.VolumePair]{})

	// Index 15 is in generation 1 (next generation): one rotation expected.
	assert.Equal(t, CacheHit, ac.CheckCache(15, keyInGen0),
		"Gen0-hit at next-gen horizon still reaches apply via the gen0→gen1 fallback")
	assert.Equal(t, CacheMiss, ac.CheckCache(15, keyInGen1),
		"Gen1-only at next-gen horizon is discarded by rotation")
}

func TestAttributeCache_CheckCache_TwoGenerationsAhead(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	ac := cache.Volumes
	cache.SetCurrentGeneration(0)

	key := attributes.NewU128(1, 1)
	ac.Put(key, attributes.Entry[*raftcmdpb.VolumePair]{})

	// Index 25 is in generation 2 (two generations ahead): any preload
	// computed now would be rotated out before apply. CheckCache surfaces
	// CacheUnreachable so admission rejects the proposal transiently.
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

	cache.ResetWithThreshold(200, 0)
	assert.Equal(t, uint64(2), cache.Epoch())

	cache.ResetWithThreshold(300, 0)
	assert.Equal(t, uint64(3), cache.Epoch())
}

// TestCache_ResetWithThresholdAtNonZeroIndex covers the atomic realignment
// path: when the caller passes a raftIndex that falls into a non-zero
// generation under the new threshold, ResetWithThreshold must set
// currentGeneration and BaseIndex accordingly in the SAME critical section
// so admission's next CheckCache never observes a transient
// (currentGeneration=0, threshold=new) window.
func TestCache_ResetWithThresholdAtNonZeroIndex(t *testing.T) {
	t.Parallel()

	cache, err := New(100, nil)
	require.NoError(t, err)

	// raftIndex=25 with newThreshold=10 → Gen=2, BaseIndex.Gen0=genEndIndex(1, 10)=20.
	cache.ResetWithThreshold(10, 25)

	assert.Equal(t, uint64(2), cache.CurrentGeneration())
	assert.Equal(t, uint64(20), cache.BaseIndex.Gen0)
	assert.Equal(t, uint64(0), cache.BaseIndex.Gen1)
	assert.Equal(t, uint64(2), cache.Epoch())
}

// TestAttributeCache_CheckCache_StaleBehindReportsMiss: when the caller's
// nextIndex maps to a generation the FSM has already left behind
// (actualGeneration > futureGeneration), the uint64 subtraction would
// otherwise underflow into the CacheUnreachable default branch. Guard
// against that: report CacheMiss so higher-level staleness checks
// (checkStaleProposal / AcquireProposalGuard) handle the actual mismatch,
// instead of falsely returning a horizon violation.
func TestAttributeCache_CheckCache_StaleBehindReportsMiss(t *testing.T) {
	t.Parallel()

	c, err := New(10, nil)
	require.NoError(t, err)
	c.SetCurrentGeneration(5) // FSM already at gen 5

	// nextIndex=15 → Gen(15, 10) = 1 (behind actualGeneration=5). Would
	// underflow: 1 - 5 = huge → default branch → CacheUnreachable.
	assert.Equal(t, CacheMiss, c.Volumes.CheckCache(15, attributes.NewU128(1, 1)),
		"stale-behind build must report CacheMiss, not underflow into CacheUnreachable")
}

// TestCache_ResetWithThresholdRejectsZero: threshold=0 is a config invariant
// violation — cache.New rejects it up front, and no legitimate call path can
// reach ResetWithThreshold with 0. Panicking makes the violation impossible
// to silently mask by disabling rotations (which would freeze
// currentGeneration=0 forever and break the CacheUnreachable contract).
func TestCache_ResetWithThresholdRejectsZero(t *testing.T) {
	t.Parallel()

	cache, err := New(100, nil)
	require.NoError(t, err)

	require.PanicsWithValue(t,
		"cache.ResetWithThreshold: threshold must be > 0 (invariant enforced by cache.New)",
		func() { cache.ResetWithThreshold(0, 0) },
	)
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

	// Get falls back to Gen1 after a rotation.
	_, ok := cache.Volumes.Get(key)
	assert.True(t, ok, "rotated entry still reachable via Get's Gen1 fallback")

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

	// Both keys visible via Get (keyGen0 via the Gen1 fallback).
	_, ok := cache.Volumes.Get(keyGen0)
	assert.True(t, ok, "keyGen0 in Gen1 reachable via Get's fallback")
	_, ok = cache.Volumes.Get(keyGen1)
	assert.True(t, ok, "keyGen1 was just written to Gen0")

	// Move to generation 2
	cache.CheckRotationNeeded(21)
	assert.Equal(t, uint64(2), cache.CurrentGeneration())

	// keyGen0 is fully evicted; keyGen1 is now in Gen1.
	_, ok = cache.Volumes.Get(keyGen0)
	assert.False(t, ok, "keyGen0 gone after second rotation")
	_, ok = cache.Volumes.Get(keyGen1)
	assert.True(t, ok, "keyGen1 still reachable via Gen1 fallback after the second rotation")
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

// TestCache_SetGenerationThresholdRejectsZero locks the invariant that a
// running cache never observes threshold=0. cache.New rejects it, and every
// setter (SetGenerationThreshold, ResetWithThreshold) panics loudly rather
// than silently disabling rotations.
func TestCache_SetGenerationThresholdRejectsZero(t *testing.T) {
	t.Parallel()

	cache, err := New(10, nil)
	require.NoError(t, err)

	require.PanicsWithValue(t,
		"cache.SetGenerationThreshold: threshold must be > 0 (invariant enforced by cache.New)",
		func() { cache.SetGenerationThreshold(0) },
	)
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

	// After rotation, Get's gen0→gen1 fallback still surfaces the entry
	// (Gen0 is fresh empty, Gen1 holds what used to be Gen0).
	_, ok := cache.Volumes.Get(key)
	assert.True(t, ok, "Volumes still reachable via gen1 fallback after first rotation")
	_, ok = cache.AccountMetadata.Get(key)
	assert.True(t, ok, "AccountMetadata still reachable via gen1 fallback after first rotation")

	// Trigger another rotation — the old Gen1 (which was Gen0) is now
	// discarded and both generations are empty.
	cache.CheckRotationNeeded(21)

	_, ok = cache.Volumes.Get(key)
	assert.False(t, ok)
	_, ok = cache.AccountMetadata.Get(key)
	assert.False(t, ok)
}
