package check

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogRangeSetAdd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		adds  [][2]uint64
		want  []logRange
		total uint64
	}{
		{
			name:  "single range",
			adds:  [][2]uint64{{1, 3}},
			want:  []logRange{{min: 1, max: 3}},
			total: 3,
		},
		{
			name:  "adjacent ranges coalesce",
			adds:  [][2]uint64{{1, 3}, {4, 6}},
			want:  []logRange{{min: 1, max: 6}},
			total: 6,
		},
		{
			name:  "overlapping ranges coalesce",
			adds:  [][2]uint64{{1, 5}, {3, 8}},
			want:  []logRange{{min: 1, max: 8}},
			total: 8,
		},
		{
			name:  "disjoint ranges stay separate",
			adds:  [][2]uint64{{1, 3}, {7, 9}},
			want:  []logRange{{min: 1, max: 3}, {min: 7, max: 9}},
			total: 6,
		},
		{
			name:  "out of order adds are sorted",
			adds:  [][2]uint64{{7, 9}, {1, 3}},
			want:  []logRange{{min: 1, max: 3}, {min: 7, max: 9}},
			total: 6,
		},
		{
			name:  "single sequence range",
			adds:  [][2]uint64{{5, 5}},
			want:  []logRange{{min: 5, max: 5}},
			total: 1,
		},
		{
			name:  "zero range is ignored (proposal produced no logs)",
			adds:  [][2]uint64{{0, 0}},
			want:  nil,
			total: 0,
		},
		{
			name:  "inverted range is ignored",
			adds:  [][2]uint64{{9, 4}},
			want:  nil,
			total: 0,
		},
		{
			name:  "nested range does not shrink the parent",
			adds:  [][2]uint64{{1, 10}, {3, 4}},
			want:  []logRange{{min: 1, max: 10}},
			total: 10,
		},
		{
			name:  "empty set",
			adds:  nil,
			want:  nil,
			total: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var s logRangeSet
			for _, a := range tc.adds {
				s.add(a[0], a[1])
			}

			require.Equal(t, tc.want, s.intervals())
			require.Equal(t, tc.total, s.total())
		})
	}
}

func TestLogRangeSetContains(t *testing.T) {
	t.Parallel()

	var s logRangeSet
	s.add(10, 12)
	s.add(20, 20)

	require.False(t, s.contains(9))
	require.True(t, s.contains(10))
	require.True(t, s.contains(11))
	require.True(t, s.contains(12))
	require.False(t, s.contains(13))
	require.True(t, s.contains(20))
	require.False(t, s.contains(21))
}

func TestLogRangeSetContainsEmpty(t *testing.T) {
	t.Parallel()

	var s logRangeSet

	require.False(t, s.contains(0))
	require.False(t, s.contains(1))
	require.True(t, s.empty())
}

// TestLogRangeSetAddAfterNormalize pins the cache-invalidation contract: a read
// normalizes and sets the flag, so a subsequent add MUST clear it or every
// later read returns state frozen at the first read.
func TestLogRangeSetAddAfterNormalize(t *testing.T) {
	t.Parallel()

	var s logRangeSet
	s.add(1, 3)

	// Force normalization via a read.
	require.True(t, s.contains(2))
	require.Equal(t, uint64(3), s.total())

	// Add after that read must be visible to every subsequent read.
	s.add(10, 12)

	require.True(t, s.contains(11), "an add after a read must be visible to contains")
	require.Equal(t, uint64(6), s.total(), "an add after a read must be counted by total")
	require.Equal(t, []logRange{{min: 1, max: 3}, {min: 10, max: 12}}, s.intervals())

	// An add that extends an existing interval must also invalidate.
	s.add(4, 5)

	require.True(t, s.contains(4))
	require.Equal(t, []logRange{{min: 1, max: 5}, {min: 10, max: 12}}, s.intervals())
}

// TestLogRangeSetChainedCoalesce covers three or more ranges collapsing
// transitively — the healthy-store shape, where every successful proposal's
// range is adjacent to the previous one and the whole set must become a single
// interval.
func TestLogRangeSetChainedCoalesce(t *testing.T) {
	t.Parallel()

	var s logRangeSet
	s.add(1, 2)
	s.add(3, 4)
	s.add(5, 6)
	s.add(7, 7)

	require.Equal(t, []logRange{{min: 1, max: 7}}, s.intervals(),
		"adjacent ranges must collapse transitively, not just pairwise")
	require.Equal(t, uint64(7), s.total())
}

// TestLogRangeSetNormalizeIdempotent verifies repeated reads do not corrupt the
// set. normalize() coalesces in place over its own backing array, so a second
// pass over already-merged state must be a no-op rather than re-processing it.
func TestLogRangeSetNormalizeIdempotent(t *testing.T) {
	t.Parallel()

	var s logRangeSet
	s.add(5, 6)
	s.add(1, 2)
	s.add(3, 4)

	first := s.intervals()
	require.Equal(t, []logRange{{min: 1, max: 6}}, first)

	// Repeated reads must return identical state.
	for i := 0; i < 3; i++ {
		require.Equal(t, []logRange{{min: 1, max: 6}}, s.intervals())
		require.Equal(t, uint64(6), s.total())
		require.False(t, s.empty())
	}
}
