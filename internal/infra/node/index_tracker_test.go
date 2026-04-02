package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexTracker(t *testing.T) {
	t.Parallel()

	t.Run("Increment and Next", func(t *testing.T) {
		t.Parallel()
		tracker := NewIndexTracker(10)
		require.Equal(t, uint64(10), tracker.Next())
		tracker.Increment(3)
		require.Equal(t, uint64(13), tracker.Next())
	})

	t.Run("Decrement", func(t *testing.T) {
		t.Parallel()
		tracker := NewIndexTracker(10)
		tracker.Increment(5)
		require.Equal(t, uint64(15), tracker.Next())
		tracker.Decrement(2)
		require.Equal(t, uint64(13), tracker.Next())
	})

	t.Run("Advance only moves forward", func(t *testing.T) {
		t.Parallel()
		tracker := NewIndexTracker(10)
		tracker.Advance(5) // below current, no-op
		require.Equal(t, uint64(10), tracker.Next())
		tracker.Advance(20) // above current, advances
		require.Equal(t, uint64(20), tracker.Next())
	})

	t.Run("Increment then Decrement restores original", func(t *testing.T) {
		t.Parallel()
		tracker := NewIndexTracker(100)
		tracker.Increment(1)
		tracker.Increment(1)
		tracker.Increment(1)
		require.Equal(t, uint64(103), tracker.Next())
		tracker.Decrement(1)
		tracker.Decrement(1)
		require.Equal(t, uint64(101), tracker.Next())
	})
}
