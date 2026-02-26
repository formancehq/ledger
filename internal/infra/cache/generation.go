package cache

func gen(i, k uint64) uint64 {
	// Raft indexes start at 1
	if i == 0 {
		return 0
	}
	return (i - 1) / k
}

func genStartIndex(g, k uint64) uint64 { return g*k + 1 }

func genEndIndex(g, k uint64) uint64 { return (g + 1) * k }

// Canonical boundary B(i) = end(gen(i)-1) for g>=1 else 0
func BoundaryIndex(i, k uint64) uint64 {
	g := gen(i, k)
	if g == 0 {
		return 0
	}
	return genEndIndex(g-1, k)
}

type DualGen[T any] struct {
	Gen0 T
	Gen1 T
}

func (d *DualGen[T]) Rotate(newGen T) *DualGen[T] {
	cp := *d
	d.Gen1, d.Gen0 = d.Gen0, newGen
	return &cp
}

func (d *DualGen[T]) Update(fn func(T)) {
	fn(d.Gen0)
}

func newDualGen[T any](gen0, gen1 T) DualGen[T] {
	return DualGen[T]{
		Gen0: gen0,
		Gen1: gen1,
	}
}
