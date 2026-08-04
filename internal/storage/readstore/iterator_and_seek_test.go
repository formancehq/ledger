package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// AndIterator.SeekGE must reposition EVERY child, not just the first. After a
// forward pass advances/exhausts a non-first child, an absolute SeekGE back to a
// smaller target must not let that child's stale (higher) Current() become the
// convergence candidate — doing so skips valid intersections below it (EN-1597,
// NumaryBot review of PR #1635).
func TestAndIterator_SeekGERepositionsAllChildren(t *testing.T) {
	t.Parallel()

	left := newAliasingIter("a", "b", "c", "z")
	right := newAliasingIter("a", "b", "c")

	it := NewAndIterator(left, right)
	defer it.Close()

	// Forward pass: intersection is {a,b,c}; the merge then exhausts (left at z,
	// right run out), leaving right's Current() stale at "c".
	var got []string
	for it.Next() {
		got = append(got, string(it.Current()))
	}
	require.Equal(t, []string{"a", "b", "c"}, got)

	// Absolute re-seek back to "a" must yield "a" — both children hold it.
	require.True(t, it.SeekGE([]byte("a")))
	require.Equal(t, "a", string(it.Current()))
}
