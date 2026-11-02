// Package generation provides callback-scoped Hegel choices for workloads.
// Each Source belongs to one Hegel callback. Do not retain it after the callback
// returns or share it with other goroutines. Concurrent episodes need separate
// RunOnce calls and separate Sources.
// Bounds match the corresponding math/rand methods, but Hegel controls the
// distribution and can favor boundary values rather than uniform sampling.
package generation

import (
	"math"

	"hegel.dev/go/hegel"
)

// Source owns the Hegel test case used for every draw. Its zero value is not usable.
type Source struct {
	tc hegel.TestCase
}

// NewSource wraps the current callback's test case, including *hegel.T in a
// local hegel.Test. It does not seed a separate random-number generator.
func NewSource(tc hegel.TestCase) *Source {
	return &Source{tc: tc}
}

// Intn draws in [0, n), and panics if n <= 0, like math/rand.Intn.
func (s *Source) Intn(n int) int {
	if n <= 0 {
		panic("invalid argument to Intn")
	}

	return hegel.Draw(s.tc, hegel.Integers(0, n-1))
}

// Int63n draws in [0, n), and panics if n <= 0, like math/rand.Int63n.
func (s *Source) Int63n(n int64) int64 {
	if n <= 0 {
		panic("invalid argument to Int63n")
	}

	return hegel.Draw(s.tc, hegel.Integers[int64](0, n-1))
}

// Uint64 draws across the full uint64 range, including math.MaxUint64.
func (s *Source) Uint64() uint64 {
	return s.Uint64Range(0, math.MaxUint64)
}

// Int63 draws in [0, math.MaxInt64], like math/rand.Int63.
func (s *Source) Int63() int64 {
	return hegel.Draw(s.tc, hegel.Integers[int64](0, math.MaxInt64))
}

// Bool draws a boolean with equal probability for each value. Use explicit
// boolean decisions when a geometric distribution must not inherit integer bias.
func (s *Source) Bool() bool {
	return hegel.Draw(s.tc, hegel.Booleans())
}

// Float64 draws a finite value in [0, 1), like math/rand.Float64.
// Hegel controls the distribution; draws are not guaranteed to be uniform.
func (s *Source) Float64() float64 {
	return hegel.Draw(s.tc, hegel.Floats[float64]().Min(0).Max(1).ExcludeMax().AllowNaN(false).AllowInfinity(false))
}

// Uint64Range draws in the inclusive range [lower, upper]. It panics if lower > upper.
// Equal bounds and the full uint64 range are supported without overflow.
func (s *Source) Uint64Range(lower, upper uint64) uint64 {
	return hegel.Draw(s.tc, hegel.Integers(lower, upper))
}

// Choice selects a pool entry by index. Repeated entries retain their weight;
// the pool is never deduplicated. An empty pool panics, like Intn(0).
func Choice[T any](source *Source, pool []T) T {
	return pool[source.Intn(len(pool))]
}
