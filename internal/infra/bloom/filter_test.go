package bloom

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testRNG returns a deterministic RNG for reproducible tests.
func testRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed^0xDEADBEEF))
}

func TestBlockedFilter_AddAndHas(t *testing.T) {
	t.Parallel()

	f := newBlockedFilterOptimized(10000, 0.01)
	rng := testRNG(1)

	keys := make([]uint64, 1000)
	for i := range keys {
		keys[i] = rng.Uint64()
		f.Add(keys[i])
	}

	// All added keys must be found (no false negatives).
	for _, key := range keys {
		require.True(t, f.Has(key), "key %d should be present", key)
	}
}

func TestBlockedFilter_FalsePositiveRate(t *testing.T) {
	t.Parallel()

	const (
		capacity = 100_000
		fpRate   = 0.01
		probes   = 1_000_000
	)

	f := newBlockedFilterOptimized(capacity, fpRate)

	// Use separate RNG seeds for "present" and "not present" sets to avoid overlap.
	rngPresent := testRNG(42)
	rngAbsent := testRNG(99)

	for range capacity {
		f.Add(rngPresent.Uint64())
	}

	falsePositives := 0
	for range probes {
		if f.Has(rngAbsent.Uint64()) {
			falsePositives++
		}
	}

	observedRate := float64(falsePositives) / probes

	// Allow 3x the target FPR as margin for blocked bloom filters.
	assert.Less(t, observedRate, fpRate*3,
		"observed FP rate %.4f exceeds 3x target %.4f", observedRate, fpRate)
}

func TestBlockedFilter_AddReturnsBlockIndex(t *testing.T) {
	t.Parallel()

	f := newBlockedFilter(BlockBits*64, 5) // 64 blocks

	idx := f.Add(0xDEADBEEF_CAFEBABE)
	require.Less(t, idx, uint32(64), "block index should be within range")

	// Same hash should always map to the same block.
	idx2 := f.Add(0xDEADBEEF_CAFEBABE)
	require.Equal(t, idx, idx2)
}

func TestBlockedFilter_SetBlock_Roundtrip(t *testing.T) {
	t.Parallel()

	rng := testRNG(4)

	src := newBlockedFilterOptimized(10000, 0.01)
	keys := make([]uint64, 500)
	for i := range keys {
		keys[i] = rng.Uint64()
		src.Add(keys[i])
	}

	// Copy all blocks to a new filter.
	dst := newBlockedFilter(src.NumBits(), src.K())
	for blockIdx := range src.BlockCount() {
		dst.SetBlock(blockIdx, src.blocks[blockIdx])
	}

	// All keys from src must be found in dst.
	for _, key := range keys {
		require.True(t, dst.Has(key), "key %d should be present in restored filter", key)
	}
}

func TestBlockedFilter_GetBlock(t *testing.T) {
	t.Parallel()

	f := newBlockedFilter(BlockBits*16, 5)

	var blk block
	blk[0] = 0xDEADBEEF
	blk[15] = 0xCAFEBABE
	f.SetBlock(3, blk)

	got := f.GetBlock(3)
	require.Equal(t, blk, got)
}

func TestBlockMarshalRoundtrip(t *testing.T) {
	t.Parallel()

	var original block
	for i := range blockWords {
		original[i] = uint32(i*0x11111111 + 0xDEAD0000)
	}

	data := marshalBlock(&original)
	require.Equal(t, blockBytes, len(data))

	restored := unmarshalBlock(data)
	require.Equal(t, original, restored)
}

func TestOptimize(t *testing.T) {
	t.Parallel()

	nbits, nhashes := optimize(100_000, 0.01)

	require.Greater(t, nbits, uint64(0))
	require.True(t, nbits%BlockBits == 0, "nbits should be a multiple of BlockBits")
	require.GreaterOrEqual(t, nhashes, 2)
}

func TestOptimize_SmallCapacity(t *testing.T) {
	t.Parallel()

	nbits, nhashes := optimize(1, 0.5)

	require.GreaterOrEqual(t, nbits, uint64(BlockBits))
	require.GreaterOrEqual(t, nhashes, 2)
}

// TestBlockedFilter_ConcurrentSetBlockAndHas verifies that SetBlock and Has
// can run concurrently without data races. This reproduces the race between
// background bloom restore (SetBlock) and preloader lookups (Has).
// TestBlockedFilter_OrBlockPreservesConcurrentAdds verifies that OrBlock does
// not destroy bits set by concurrent Add calls. This is the fix for the
// RestoreFromStore vs FSM AddCanonicalKeys race that caused false negatives
// ("insufficient funds") after restart.
func TestBlockedFilter_OrBlockPreservesConcurrentAdds(t *testing.T) {
	t.Parallel()

	const nblocks = 64
	f := newBlockedFilter(BlockBits*nblocks, 5)

	// Keys added by the "FSM goroutine" during restore.
	rng := testRNG(99)
	addedKeys := make([]uint64, 500)
	for i := range addedKeys {
		addedKeys[i] = rng.Uint64()
	}

	// Persisted blocks: a zeroed filter (simulates empty Pebble bloom state).
	var zeroBlock block

	done := make(chan struct{})

	// Background goroutine: simulates RestoreFromStore calling OrBlock.
	go func() {
		defer close(done)

		for range 50 {
			for blockIdx := range uint64(nblocks) {
				f.OrBlock(blockIdx, zeroBlock)
			}
		}
	}()

	// FSM goroutine: concurrently adds keys (simulates AddCanonicalKeys).
	for _, k := range addedKeys {
		f.Add(k)
	}

	<-done

	// All keys added by Add must still be present — no false negatives.
	for i, k := range addedKeys {
		assert.True(t, f.Has(k), "key %d (0x%x) lost after concurrent OrBlock", i, k)
	}
}

// Run with -race to detect: go test -race -run TestBlockedFilter_ConcurrentSetBlockAndHas.
func TestBlockedFilter_ConcurrentSetBlockAndHas(t *testing.T) {
	t.Parallel()

	f := newBlockedFilter(BlockBits*64, 5)

	// Pre-populate some blocks with known data.
	var blk block
	for i := range blockWords {
		blk[i] = 0xFFFFFFFF
	}

	done := make(chan struct{})

	// Writer goroutine: continuously sets blocks (simulates RestoreFromStore).
	go func() {
		defer close(done)

		for i := range 10_000 {
			blockIdx := uint64(i) % f.BlockCount()
			f.SetBlock(blockIdx, blk)
		}
	}()

	// Reader goroutine (this goroutine): continuously checks membership (simulates preloader).
	rng := testRNG(77)
	for range 10_000 {
		f.Has(rng.Uint64())
	}

	<-done
}
