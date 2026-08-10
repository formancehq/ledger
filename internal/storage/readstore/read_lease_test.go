package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// BeginGC lowers a proposed watermark to the minimum live pin, so a
// registered reader keeps every event its pin can still resolve.
func TestLeaseRegistry_BeginGCRespectsLivePins(t *testing.T) {
	t.Parallel()

	r := NewLeaseRegistry()

	a, ok := r.Acquire(10)
	require.True(t, ok)

	b, ok := r.Acquire(25)
	require.True(t, ok)

	require.Equal(t, uint64(10), r.BeginGC(100), "the lowest live pin bounds the sweep")
	require.Equal(t, uint64(10), r.ReclaimFloor(), "the floor never passes a live pin")

	a.Release()
	a.Release() // idempotent

	require.Equal(t, uint64(25), r.BeginGC(100))

	b.Release()
	require.Equal(t, uint64(100), r.BeginGC(100))
}

// A pin below the reclaim floor is refused: the events it would resolve may
// already be gone, so the reader must re-pin rather than read a truncated
// group.
func TestLeaseRegistry_AcquireRefusesBelowFloor(t *testing.T) {
	t.Parallel()

	r := NewLeaseRegistry()

	require.Equal(t, uint64(50), r.BeginGC(50))
	require.Equal(t, uint64(50), r.ReclaimFloor())

	_, ok := r.Acquire(49)
	require.False(t, ok, "reclamation has passed 49")

	lease, ok := r.Acquire(50)
	require.True(t, ok, "the floor itself is still resolvable")
	lease.Release()
}

// A live pin holds the floor down for as long as it is registered, so a pass
// running while it is held cannot invalidate it — and once released, the
// floor resumes advancing with the fold cursor.
func TestLeaseRegistry_LivePinHoldsTheFloor(t *testing.T) {
	t.Parallel()

	r := NewLeaseRegistry()

	held, ok := r.Acquire(60)
	require.True(t, ok)

	require.Equal(t, uint64(60), r.BeginGC(200), "the sweep cannot pass the held pin")
	require.Equal(t, uint64(60), r.ReclaimFloor())

	// The holder is still admissible at its own pin for the whole lease.
	again, ok := r.Acquire(60)
	require.True(t, ok)
	again.Release()

	held.Release()

	require.Equal(t, uint64(200), r.BeginGC(200), "released: the fold cursor stands again")

	_, ok = r.Acquire(60)
	require.False(t, ok, "reclamation has now passed 60")
}
