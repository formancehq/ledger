package readstore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// sliceIter is a minimal sorted EntityIterator over an in-memory byte-slice
// list. Used to drive AndIterator in tests without depending on Pebble.
type sliceIter struct {
	rows [][]byte
	idx  int
}

func (s *sliceIter) Next() bool {
	if s.idx >= len(s.rows) {
		return false
	}

	s.idx++

	return s.idx <= len(s.rows)
}

func (s *sliceIter) Current() []byte {
	if s.idx == 0 || s.idx > len(s.rows) {
		return nil
	}

	return s.rows[s.idx-1]
}

func (s *sliceIter) SeekGE(target []byte) bool {
	for s.idx < len(s.rows) {
		s.idx++

		if cmp := compareBytes(s.rows[s.idx-1], target); cmp >= 0 {
			return true
		}
	}

	return false
}

func (s *sliceIter) Close() {}

func compareBytes(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}

	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}

	return 0
}

var _ readstore.EntityIterator = (*sliceIter)(nil)

// TestAndIterator_OnSkip_FiresOncePerDiscardedCandidate seeds two sorted
// child iterators with one shared row and several misaligned rows, then
// counts how many converge iterations rejected a candidate before the match.
func TestAndIterator_OnSkip_FiresOncePerDiscardedCandidate(t *testing.T) {
	t.Parallel()

	// Children overlap only at {0x05}. Each non-matching row in either child
	// forces converge() to discard a candidate.
	left := &sliceIter{rows: [][]byte{{0x01}, {0x03}, {0x05}}}
	right := &sliceIter{rows: [][]byte{{0x02}, {0x04}, {0x05}}}

	and := readstore.NewAndIterator(left, right)

	var skips int

	and.SetOnSkip(func() { skips++ })

	require.True(t, and.Next(), "intersection must yield 0x05")
	assert.Equal(t, []byte{0x05}, and.Current())
	assert.False(t, and.Next(), "no further intersection")

	assert.Greater(t, skips, 0, "onSkip should fire when converge discards a candidate")
}

func TestAndIterator_OnSkip_Disabled_NoPanic(t *testing.T) {
	t.Parallel()

	left := &sliceIter{rows: [][]byte{{0x01}, {0x02}}}
	right := &sliceIter{rows: [][]byte{{0x02}, {0x03}}}

	and := readstore.NewAndIterator(left, right) // SetOnSkip never called

	require.True(t, and.Next())
	assert.Equal(t, []byte{0x02}, and.Current())
}
