package query_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/query"
)

// returnsInOrder builds a closure that returns each bool from results in
// turn, then false forever — matching the old hand-rolled slice-driven
// behavior.
func returnsInOrder(results []bool) func() bool {
	i := 0

	return func() bool {
		if i >= len(results) {
			return false
		}

		r := results[i]
		i++

		return r
	}
}

func TestTrackedIterator_CountsNextCalls(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	inner := NewMockEntityIterator(ctrl)
	inner.EXPECT().Next().DoAndReturn(returnsInOrder([]bool{true, true, false})).Times(3)

	stats := &query.IteratorStats{}
	tracked := query.NewTrackedIterator(inner, stats)

	require.True(t, tracked.Next())
	require.True(t, tracked.Next())
	require.False(t, tracked.Next())

	assert.Equal(t, int64(3), stats.NextCalls)
	assert.Equal(t, int64(0), stats.SeekCalls)
}

func TestTrackedIterator_CountsSeekGECalls(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	inner := NewMockEntityIterator(ctrl)
	seek := returnsInOrder([]bool{true, false})
	inner.EXPECT().SeekGE(gomock.Any()).DoAndReturn(func(_ []byte) bool { return seek() }).Times(2)

	stats := &query.IteratorStats{}
	tracked := query.NewTrackedIterator(inner, stats)

	require.True(t, tracked.SeekGE([]byte("abc")))
	require.False(t, tracked.SeekGE([]byte("xyz")))

	assert.Equal(t, int64(0), stats.NextCalls)
	assert.Equal(t, int64(2), stats.SeekCalls)
}

func TestTrackedIterator_DelegatesCurrent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	inner := NewMockEntityIterator(ctrl)
	expected := []byte("account:alice")
	inner.EXPECT().Current().Return(expected)

	stats := &query.IteratorStats{}
	tracked := query.NewTrackedIterator(inner, stats)

	assert.Equal(t, expected, tracked.Current())
}

func TestTrackedIterator_DelegatesClose(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	inner := NewMockEntityIterator(ctrl)
	inner.EXPECT().Close()

	stats := &query.IteratorStats{}
	tracked := query.NewTrackedIterator(inner, stats)

	tracked.Close()
}

func TestTrackedIterator_CountsItemsEmittedOnSuccessfulNext(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	inner := NewMockEntityIterator(ctrl)
	inner.EXPECT().Next().DoAndReturn(returnsInOrder([]bool{true, true, false, true})).Times(4)

	stats := &query.IteratorStats{}
	tracked := query.NewTrackedIterator(inner, stats)

	for range 4 {
		tracked.Next()
	}

	assert.Equal(t, int64(4), stats.NextCalls)
	assert.Equal(t, int64(3), stats.ItemsEmitted, "ItemsEmitted should match Next() returning true")
}

func TestTrackedIterator_AccumulatesDuration(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	inner := NewMockEntityIterator(ctrl)
	next := returnsInOrder([]bool{true, false})
	inner.EXPECT().Next().DoAndReturn(func() bool { return next() }).Times(2)
	inner.EXPECT().SeekGE(gomock.Any()).Return(true)

	stats := &query.IteratorStats{}
	tracked := query.NewTrackedIterator(inner, stats)

	tracked.SeekGE([]byte("k"))
	tracked.Next()
	tracked.Next()

	assert.Greater(t, stats.Duration, time.Duration(0), "Duration should advance after Next/SeekGE calls")
}

func TestTrackedIterator_MixedOperations(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	inner := NewMockEntityIterator(ctrl)
	next := returnsInOrder([]bool{true, true})
	inner.EXPECT().Next().DoAndReturn(func() bool { return next() }).Times(2)
	inner.EXPECT().SeekGE(gomock.Any()).Return(true)

	stats := &query.IteratorStats{
		Label: "test",
		Kind:  "Prefix",
	}
	tracked := query.NewTrackedIterator(inner, stats)

	tracked.SeekGE([]byte("start"))
	tracked.Next()
	tracked.Next()

	assert.Equal(t, int64(2), stats.NextCalls)
	assert.Equal(t, int64(1), stats.SeekCalls)
	assert.Equal(t, "test", stats.Label)
	assert.Equal(t, "Prefix", stats.Kind)
}
