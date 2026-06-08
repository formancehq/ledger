package query_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// mockEntityIterator is a minimal EntityIterator for testing TrackedIterator.
type mockEntityIterator struct {
	nextResults []bool
	seekResults []bool
	nextIdx     int
	seekIdx     int
	currentVal  []byte
	closeCalled bool
}

func (m *mockEntityIterator) Next() bool {
	if m.nextIdx >= len(m.nextResults) {
		return false
	}

	result := m.nextResults[m.nextIdx]
	m.nextIdx++

	return result
}

func (m *mockEntityIterator) Current() []byte {
	return m.currentVal
}

func (m *mockEntityIterator) SeekGE(_ []byte) bool {
	if m.seekIdx >= len(m.seekResults) {
		return false
	}

	result := m.seekResults[m.seekIdx]
	m.seekIdx++

	return result
}

func (m *mockEntityIterator) Close() {
	m.closeCalled = true
}

var _ readstore.EntityIterator = (*mockEntityIterator)(nil)

func TestTrackedIterator_CountsNextCalls(t *testing.T) {
	t.Parallel()

	inner := &mockEntityIterator{
		nextResults: []bool{true, true, false},
	}
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

	inner := &mockEntityIterator{
		seekResults: []bool{true, false},
	}
	stats := &query.IteratorStats{}
	tracked := query.NewTrackedIterator(inner, stats)

	require.True(t, tracked.SeekGE([]byte("abc")))
	require.False(t, tracked.SeekGE([]byte("xyz")))

	assert.Equal(t, int64(0), stats.NextCalls)
	assert.Equal(t, int64(2), stats.SeekCalls)
}

func TestTrackedIterator_DelegatesCurrent(t *testing.T) {
	t.Parallel()

	expected := []byte("account:alice")
	inner := &mockEntityIterator{currentVal: expected}
	stats := &query.IteratorStats{}
	tracked := query.NewTrackedIterator(inner, stats)

	assert.Equal(t, expected, tracked.Current())
}

func TestTrackedIterator_DelegatesClose(t *testing.T) {
	t.Parallel()

	inner := &mockEntityIterator{}
	stats := &query.IteratorStats{}
	tracked := query.NewTrackedIterator(inner, stats)

	tracked.Close()
	assert.True(t, inner.closeCalled)
}

func TestTrackedIterator_CountsItemsEmittedOnSuccessfulNext(t *testing.T) {
	t.Parallel()

	inner := &mockEntityIterator{
		nextResults: []bool{true, true, false, true},
	}
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

	inner := &mockEntityIterator{
		nextResults: []bool{true, false},
		seekResults: []bool{true},
	}
	stats := &query.IteratorStats{}
	tracked := query.NewTrackedIterator(inner, stats)

	tracked.SeekGE([]byte("k"))
	tracked.Next()
	tracked.Next()

	assert.Greater(t, stats.Duration, time.Duration(0), "Duration should advance after Next/SeekGE calls")
}

func TestTrackedIterator_MixedOperations(t *testing.T) {
	t.Parallel()

	inner := &mockEntityIterator{
		nextResults: []bool{true, true},
		seekResults: []bool{true},
		currentVal:  []byte("entity"),
	}
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
