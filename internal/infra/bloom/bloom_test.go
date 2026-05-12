package bloom

import (
	"encoding/binary"
	"maps"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

func newTestFilter(t *testing.T) *Filter {
	t.Helper()

	meter := noop.NewMeterProvider().Meter("test")

	return newFilter(10000, 0.01, dal.AttributeCodeVolume, meter, "test")
}

func hashKey(key []byte) attributes.U128 {
	return attributes.HashU128(attributes.DefaultSeeds, key)
}

func TestFilter_DirtyTracking(t *testing.T) {
	t.Parallel()

	f := newTestFilter(t)

	// Initially no dirty blocks.
	count := 0
	for range f.dirtyBlocks() {
		count++
	}

	require.Equal(t, 0, count, "should have no dirty blocks initially")

	// Add a key; should dirty exactly one block.
	f.Add(hashKey([]byte("test-key")))

	dirtyIndices := make(map[uint64]bool)
	for idx := range f.dirtyBlocks() {
		dirtyIndices[idx] = true
	}

	require.Equal(t, 1, len(dirtyIndices), "should have exactly one dirty block")

	// Clear resets the dirty set.
	clear(f.dirty)

	count = 0
	for range f.dirtyBlocks() {
		count++
	}

	require.Equal(t, 0, count, "should have no dirty blocks after clear")
}

func TestFilter_DirtyBlocks_MultipleBlocks(t *testing.T) {
	t.Parallel()

	f := newTestFilter(t)
	rng := rand.New(rand.NewPCG(3, 3^0xDEADBEEF))

	for range 1000 {
		var buf [16]byte
		binary.LittleEndian.PutUint64(buf[:8], rng.Uint64())
		binary.LittleEndian.PutUint64(buf[8:], rng.Uint64())
		f.Add(hashKey(buf[:]))
	}

	dirty := maps.Collect(f.dirtyBlocks())

	require.Greater(t, len(dirty), 1, "should have multiple dirty blocks after many inserts")

	for idx, blk := range dirty {
		nonZero := false
		for _, w := range blk {
			if w != 0 {
				nonZero = true

				break
			}
		}

		require.True(t, nonZero, "dirty block %d should have non-zero data", idx)
	}
}

func TestFilter_PersistAndRestore(t *testing.T) {
	t.Parallel()

	f := newTestFilter(t)
	rng := rand.New(rand.NewPCG(5, 5^0xDEADBEEF))

	keys := make([]attributes.U128, 500)
	for i := range keys {
		var buf [16]byte
		binary.LittleEndian.PutUint64(buf[:8], rng.Uint64())
		binary.LittleEndian.PutUint64(buf[8:], rng.Uint64())
		keys[i] = hashKey(buf[:])
		f.Add(keys[i])
	}

	// Simulate persist: collect dirty blocks via marshal.
	persisted := make(map[uint64][]byte)
	for idx, blk := range f.dirtyBlocks() {
		persisted[idx] = marshalBlock(&blk)
	}

	clear(f.dirty)

	// Add more keys after the flush.
	for range 200 {
		var buf [16]byte
		binary.LittleEndian.PutUint64(buf[:8], rng.Uint64())
		binary.LittleEndian.PutUint64(buf[8:], rng.Uint64())
		f.Add(hashKey(buf[:]))
	}

	// Simulate restore into a fresh filter (only the flushed blocks).
	restored := newBlockedFilter(f.filter.NumBits(), f.filter.K())
	for idx, data := range persisted {
		restored.SetBlock(idx, unmarshalBlock(data))
	}

	// Keys from before the flush must be found.
	for _, key := range keys {
		require.True(t, restored.Has(key.Hi()), "pre-flush key should be present")
	}
}
