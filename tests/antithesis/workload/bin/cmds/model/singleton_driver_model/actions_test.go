package main

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// The admission boundary enforces ^[A-Z]*$ on a posting's color, so every name
// the generator can produce — including the deepest bucket a 64-bit draw
// reaches — has to land inside that charset.
func TestColorName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "", colorName(0), "the uncolored bucket")
	require.Equal(t, "A", colorName(1))
	require.Equal(t, "Z", colorName(26))
	require.Equal(t, "AA", colorName(27))
	require.Equal(t, "BL", colorName(64), "TrailingZeros64's ceiling")

	charset := regexp.MustCompile(`^[A-Z]*$`)
	seen := map[string]bool{}
	for k := range 65 {
		name := colorName(k)
		require.Regexp(t, charset, name)
		require.False(t, seen[name], "colorName(%d) collides with an earlier bucket", k)
		seen[name] = true
	}
}

// The draw is geometric with p=1/2: the uncolored bucket takes half, and the
// tail stays reachable. Bounds are wide enough that the sampling noise over
// this many draws cannot reach them.
func TestRandomColor_Distribution(t *testing.T) {
	t.Parallel()

	const draws = 20000

	counts := map[string]int{}
	for range draws {
		counts[randomColor()]++
	}

	require.InDelta(t, draws/2, counts[""], draws/10, `"" is half the draws`)
	require.InDelta(t, draws/4, counts["A"], draws/10, `"A" is a quarter`)
	require.NotZero(t, counts["C"], "the tail past the first few buckets is reachable")
}
