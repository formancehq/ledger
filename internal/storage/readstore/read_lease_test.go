package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLeaseRegistry(t *testing.T) {
	t.Parallel()

	r := NewLeaseRegistry()

	_, ok := r.Watermark()
	require.False(t, ok, "no live reads: caller may reclaim up to its fold cursor")

	a := r.Acquire(10)
	b := r.Acquire(25)

	w, ok := r.Watermark()
	require.True(t, ok)
	require.Equal(t, uint64(10), w)

	a.Release()
	a.Release() // idempotent

	w, ok = r.Watermark()
	require.True(t, ok)
	require.Equal(t, uint64(25), w)

	b.Release()
	_, ok = r.Watermark()
	require.False(t, ok)
}
