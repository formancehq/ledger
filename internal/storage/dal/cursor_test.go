package dal

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSliceCursor_Next(t *testing.T) {
	t.Parallel()

	cursor := NewSliceCursor([]int{1, 2, 3})

	v, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 1, v)

	v, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 2, v)

	v, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 3, v)

	_, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestSliceCursor_Empty(t *testing.T) {
	t.Parallel()

	cursor := NewSliceCursor([]string{})

	_, err := cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestSliceCursor_Nil(t *testing.T) {
	t.Parallel()

	cursor := NewSliceCursor[int](nil)

	_, err := cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestSliceCursor_Close(t *testing.T) {
	t.Parallel()

	cursor := NewSliceCursor([]int{1, 2})
	require.NoError(t, cursor.Close())
}

func TestFilteredCursor_FiltersCorrectly(t *testing.T) {
	t.Parallel()

	inner := NewSliceCursor([]int{1, 2, 3, 4, 5, 6})
	cursor := NewFilteredCursor(inner, func(v int) bool { return v%2 == 0 })

	v, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 2, v)

	v, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 4, v)

	v, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 6, v)

	_, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestFilteredCursor_FilterAll(t *testing.T) {
	t.Parallel()

	inner := NewSliceCursor([]int{1, 3, 5})
	cursor := NewFilteredCursor(inner, func(v int) bool { return v%2 == 0 })

	_, err := cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestFilteredCursor_FilterNone(t *testing.T) {
	t.Parallel()

	inner := NewSliceCursor([]int{2, 4, 6})
	cursor := NewFilteredCursor(inner, func(v int) bool { return v%2 == 0 })

	v, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 2, v)

	v, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 4, v)

	v, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 6, v)

	_, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestFilteredCursor_Close(t *testing.T) {
	t.Parallel()

	inner := NewSliceCursor([]int{1, 2, 3})
	cursor := NewFilteredCursor(inner, func(v int) bool { return true })
	require.NoError(t, cursor.Close())
}

func TestLimitedCursor_LimitsCorrectly(t *testing.T) {
	t.Parallel()

	inner := NewSliceCursor([]int{1, 2, 3, 4, 5})
	cursor := NewLimitedCursor(inner, 3)

	v, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 1, v)

	v, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 2, v)

	v, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 3, v)

	_, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestLimitedCursor_LimitZero(t *testing.T) {
	t.Parallel()

	inner := NewSliceCursor([]int{1, 2, 3})
	cursor := NewLimitedCursor(inner, 0)

	_, err := cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestLimitedCursor_LimitExceedsAvailable(t *testing.T) {
	t.Parallel()

	inner := NewSliceCursor([]int{1, 2})
	cursor := NewLimitedCursor(inner, 10)

	v, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 1, v)

	v, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 2, v)

	_, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestLimitedCursor_Close(t *testing.T) {
	t.Parallel()

	inner := NewSliceCursor([]int{1, 2, 3})
	cursor := NewLimitedCursor(inner, 2)
	require.NoError(t, cursor.Close())
}
