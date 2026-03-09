package attributes

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexGuard_MinNoReaders(t *testing.T) {
	t.Parallel()

	g := NewIndexGuard()
	require.Equal(t, uint64(math.MaxUint64), g.Min())
}

func TestIndexGuard_HoldAndRelease(t *testing.T) {
	t.Parallel()

	g := NewIndexGuard()

	release1 := g.Hold(100)
	require.Equal(t, uint64(100), g.Min())

	release2 := g.Hold(50)
	require.Equal(t, uint64(50), g.Min())

	release3 := g.Hold(200)
	require.Equal(t, uint64(50), g.Min())

	// Release the minimum reader.
	release2()
	require.Equal(t, uint64(100), g.Min())

	release1()
	require.Equal(t, uint64(200), g.Min())

	release3()
	require.Equal(t, uint64(math.MaxUint64), g.Min())
}

func TestIndexGuard_DoubleRelease(t *testing.T) {
	t.Parallel()

	g := NewIndexGuard()

	release := g.Hold(42)
	release()
	release() // should be a no-op

	require.Equal(t, uint64(math.MaxUint64), g.Min())
}

func TestIndexGuard_DuplicateIndexValues(t *testing.T) {
	t.Parallel()

	g := NewIndexGuard()

	release1 := g.Hold(100)
	release2 := g.Hold(100)

	require.Equal(t, uint64(100), g.Min())

	release1()
	// Second reader at same index still active.
	require.Equal(t, uint64(100), g.Min())

	release2()
	require.Equal(t, uint64(math.MaxUint64), g.Min())
}

func TestIndexGuard_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	g := NewIndexGuard()

	var wg sync.WaitGroup

	for i := range 100 {
		wg.Add(1)

		go func(idx uint64) {
			defer wg.Done()

			release := g.Hold(idx)
			// Simulate some work
			_ = g.Min()
			release()
		}(uint64(i))
	}

	wg.Wait()

	require.Equal(t, uint64(math.MaxUint64), g.Min())
}
