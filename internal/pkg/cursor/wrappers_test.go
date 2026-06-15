package cursor_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
)

func collectAll(t *testing.T, c cursor.Cursor[int]) []int {
	t.Helper()

	items, err := cursor.Collect(c)
	require.NoError(t, err)

	return items
}

func TestSkipWhileCursor(t *testing.T) {
	t.Parallel()

	t.Run("skips prefix matching predicate", func(t *testing.T) {
		t.Parallel()

		inner := cursor.NewSliceCursor([]int{1, 2, 3, 4, 5})
		c := cursor.NewSkipWhileCursor(inner, func(v int) bool { return v < 3 })

		require.Equal(t, []int{3, 4, 5}, collectAll(t, c))
	})

	t.Run("passes everything through once predicate is false", func(t *testing.T) {
		t.Parallel()

		// Once the predicate yields false, subsequent items pass without re-evaluation —
		// this matches "skip while" semantics, NOT a generic filter.
		inner := cursor.NewSliceCursor([]int{1, 2, 5, 2, 1})
		c := cursor.NewSkipWhileCursor(inner, func(v int) bool { return v < 3 })

		require.Equal(t, []int{5, 2, 1}, collectAll(t, c))
	})

	t.Run("empty input", func(t *testing.T) {
		t.Parallel()

		inner := cursor.NewSliceCursor([]int{})
		c := cursor.NewSkipWhileCursor(inner, func(int) bool { return true })

		_, err := c.Next()
		require.ErrorIs(t, err, io.EOF)
	})
}

func TestReverse(t *testing.T) {
	t.Parallel()

	t.Run("reverses items", func(t *testing.T) {
		t.Parallel()

		c, err := cursor.Reverse(cursor.NewSliceCursor([]int{1, 2, 3, 4}))
		require.NoError(t, err)

		require.Equal(t, []int{4, 3, 2, 1}, collectAll(t, c))
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		c, err := cursor.Reverse(cursor.NewSliceCursor([]int{}))
		require.NoError(t, err)

		_, err = c.Next()
		require.ErrorIs(t, err, io.EOF)
	})
}
