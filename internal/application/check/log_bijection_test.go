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
