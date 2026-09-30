package grpc

import (
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUint256DecimalJSON(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		decimal string
	}{
		{"zero", "0"},
		{"one", "1"},
		{"beyond_js_integer", "9007199254740993"},
		{"maximum", "115792089237316195423570985008687907853269984665640564039457584007913129639935"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var value Uint256
			require.NoError(t, value.UnmarshalJSON([]byte(tc.decimal)))
			encoded, err := value.MarshalJSON()
			require.NoError(t, err)
			require.Equal(t, tc.decimal, string(encoded))
			require.Equal(t, tc.decimal, value.ToBigInt().String())
		})
	}
}

func TestUint256RejectsInvalidDecimalJSON(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "-1", "1.5", "\"1\"", "01", "115792089237316195423570985008687907853269984665640564039457584007913129639936", strings.Repeat("9", 100_000)} {
		var amount Uint256
		require.Error(t, amount.UnmarshalJSON([]byte(value)), value)
	}
}

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
