package generation

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"hegel.dev/go/hegel"
)

func TestSourceBounds(t *testing.T) {
	t.Parallel()
	hegel.Test(t, func(tc *hegel.T) {
		s := NewSource(tc)
		for _, n := range []int{1, 2, 17, math.MaxInt} {
			v := s.Intn(n)
			require.GreaterOrEqual(tc, v, 0)
			require.Less(tc, v, n)
		}
		for _, n := range []int64{1, 2, 17, math.MaxInt64} {
			v := s.Int63n(n)
			require.GreaterOrEqual(tc, v, int64(0))
			require.Less(tc, v, n)
		}
		require.GreaterOrEqual(tc, s.Int63(), int64(0))
		f := s.Float64()
		require.False(tc, math.IsNaN(f) || math.IsInf(f, 0))
		require.GreaterOrEqual(tc, f, 0.0)
		require.Less(tc, f, 1.0)
		for _, bounds := range [][2]uint64{
			{0, 0}, {1, 1}, {math.MaxInt64, math.MaxInt64 + 1},
			{math.MaxInt64 + 1, math.MaxUint64}, {math.MaxUint64, math.MaxUint64},
			{0, math.MaxUint64},
		} {
			v := s.Uint64Range(bounds[0], bounds[1])
			require.GreaterOrEqual(tc, v, bounds[0])
			require.LessOrEqual(tc, v, bounds[1])
		}
	}, localOptions(101)...)
}

func TestSourceInvalidBounds(t *testing.T) {
	t.Parallel()
	// Invalid arguments must panic before accessing a native test case.
	s := &Source{}
	for name, fn := range map[string]func(){
		"Intn zero":       func() { s.Intn(0) },
		"Intn negative":   func() { s.Intn(-1) },
		"Int63n zero":     func() { s.Int63n(0) },
		"Int63n negative": func() { s.Int63n(math.MinInt64) },
		"reversed range":  func() { s.Uint64Range(math.MaxUint64, 0) },
		"nil pool":        func() { Choice[int](s, nil) },
		"empty pool":      func() { Choice(s, []string{}) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Panics(t, fn)
		})
	}
}

func TestSourceUint64UsesUpperHalf(t *testing.T) {
	t.Parallel()
	var unsigned []uint64
	hegel.Test(t, func(tc *hegel.T) {
		s := NewSource(tc)
		unsigned = append(unsigned, s.Uint64())
	}, localOptions(93)...)
	upperHalf := false
	for _, v := range unsigned {
		upperHalf = upperHalf || v > math.MaxInt64
	}
	require.True(t, upperHalf, "Uint64 must not truncate to 63 bits")
}

func TestSourceSeededReproducibility(t *testing.T) {
	t.Parallel()
	type draws struct {
		Int     int
		Int63n  int64
		Uint64  uint64
		Int63   int64
		Float64 float64
		Range   uint64
		Choice  string
		Bool    bool
	}
	pool := []string{"common", "common", "rare"}
	collect := func() []draws {
		var result []draws
		hegel.Test(t, func(tc *hegel.T) {
			s := NewSource(tc)
			result = append(result, draws{
				s.Intn(math.MaxInt), s.Int63n(math.MaxInt64), s.Uint64(), s.Int63(),
				s.Float64(), s.Uint64Range(math.MaxInt64+1, math.MaxUint64), Choice(s, pool), s.Bool(),
			})
		}, localOptions(42)...)

		return result
	}
	first := collect()
	require.NotEmpty(t, first)
	require.Equal(t, first, collect())
	require.NotEqual(t, first[0].Uint64, first[len(first)-1].Uint64, "draws must not be constant")
}

func TestChoicePreservesRepeatedPool(t *testing.T) {
	t.Parallel()
	pool := []string{"common", "common", "rare"}
	var choices []string
	var indexed []string
	hegel.Test(t, func(tc *hegel.T) {
		choices = append(choices, Choice(NewSource(tc), pool))
	}, localOptions(73)...)
	hegel.Test(t, func(tc *hegel.T) {
		indexed = append(indexed, pool[NewSource(tc).Intn(len(pool))])
	}, localOptions(73)...)
	require.Equal(t, indexed, choices)
	require.Contains(t, choices, "common")
	require.Contains(t, choices, "rare")
	require.Equal(t, []string{"common", "common", "rare"}, pool)
}

func localOptions(seed int64) []hegel.Option {
	return []hegel.Option{
		hegel.WithProfile("base"), hegel.WithBackend(hegel.BackendDefault),
		hegel.WithDatabase(""), hegel.WithSeed(seed), hegel.WithTestCases(32),
		hegel.WithPhases(hegel.PhaseGenerate),
	}
}
