package state

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIdempotencyExpiresAt covers the per-outcome expiry derivation: a zero TTL
// yields a never-expiring outcome, a finite TTL adds to created_at, and an
// addition that would overflow uint64 saturates to the maximum.
func TestIdempotencyExpiresAt(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint64(0), IdempotencyExpiresAt(1000, 0), "ttl 0 never expires")
	require.Equal(t, uint64(1600), IdempotencyExpiresAt(1000, 600), "finite ttl adds to created_at")
	require.Equal(t, uint64(math.MaxUint64), IdempotencyExpiresAt(math.MaxUint64-5, 10), "overflow saturates")
}

// TestIdempotencyExpired covers the expiry predicate: a zero expires_at never
// expires, and a finite one expires exactly at its deadline.
func TestIdempotencyExpired(t *testing.T) {
	t.Parallel()

	require.False(t, IdempotencyExpired(0, math.MaxUint64), "expires_at 0 never expires")
	require.False(t, IdempotencyExpired(1000, 999), "before the deadline is live")
	require.True(t, IdempotencyExpired(1000, 1000), "at the deadline is expired")
	require.True(t, IdempotencyExpired(1000, 1001), "past the deadline is expired")
}
