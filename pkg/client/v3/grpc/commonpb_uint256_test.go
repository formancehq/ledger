package grpc

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUint256Convenience(t *testing.T) {
	t.Parallel()
	require.True(t, (*Uint256)(nil).IsZero())
	require.Equal(t, "0", (*Uint256)(nil).Dec())
	require.Equal(t, big.NewInt(0), (*Uint256)(nil).ToBigInt())
	require.True(t, (&Uint256{}).IsZero())
	require.False(t, (&Uint256{V1: 1}).IsZero())
	require.Equal(t, "42", NewUint256FromUint64(42).Dec())
	require.Equal(t, new(big.Int).Lsh(big.NewInt(1), 64), (&Uint256{V1: 1}).ToBigInt())
}
