package commonpb

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestUint256ProtoRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value *uint256.Int
	}{
		{"zero", uint256.NewInt(0)},
		{"one", uint256.NewInt(1)},
		{"small", uint256.NewInt(42)},
		{"large", uint256.NewInt(1_000_000_000)},
		{"max_uint64", new(uint256.Int).SetUint64(^uint64(0))},
		{"max_uint256", new(uint256.Int).Sub(new(uint256.Int).Lsh(uint256.NewInt(1), 256), uint256.NewInt(1))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// NewUint256 -> IntoUint256 round-trip
			proto := NewUint256(tt.value)
			var dst uint256.Int
			proto.IntoUint256(&dst)
			require.True(t, tt.value.Eq(&dst), "expected %s, got %s", tt.value.Dec(), dst.Dec())

			// SetFromUint256 round-trip
			proto2 := &Uint256{}
			proto2.SetFromUint256(tt.value)
			var dst2 uint256.Int
			proto2.IntoUint256(&dst2)
			require.True(t, tt.value.Eq(&dst2), "SetFrom: expected %s, got %s", tt.value.Dec(), dst2.Dec())
		})
	}
}

func TestUint256_ToBigInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    uint64
		expected string
	}{
		{"zero", 0, "0"},
		{"one", 1, "1"},
		{"large", 1_000_000_000, "1000000000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			proto := NewUint256FromUint64(tt.value)
			got := proto.ToBigInt()
			require.Equal(t, tt.expected, got.String())
		})
	}
}

func TestUint256_IsZero(t *testing.T) {
	t.Parallel()

	require.True(t, (*Uint256)(nil).IsZero(), "nil should be zero")
	require.True(t, (&Uint256{}).IsZero(), "empty struct should be zero")
	require.True(t, NewUint256FromUint64(0).IsZero(), "0 should be zero")
	require.False(t, NewUint256FromUint64(1).IsZero(), "1 should not be zero")
	require.False(t, (&Uint256{V1: 1}).IsZero(), "high limb set should not be zero")
}

func TestUint256_Dec(t *testing.T) {
	t.Parallel()

	require.Equal(t, "0", (*Uint256)(nil).Dec())
	require.Equal(t, "0", NewUint256FromUint64(0).Dec())
	require.Equal(t, "42", NewUint256FromUint64(42).Dec())
	require.Equal(t, "1000000000", NewUint256FromUint64(1_000_000_000).Dec())
}

func TestUint256_JSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value uint64
		json  string
	}{
		{"zero", 0, "0"},
		{"one", 1, "1"},
		{"large", 1_000_000_000, "1000000000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			proto := NewUint256FromUint64(tt.value)
			data, err := proto.MarshalJSON()
			require.NoError(t, err)
			require.Equal(t, tt.json, string(data))

			var proto2 Uint256
			err = proto2.UnmarshalJSON(data)
			require.NoError(t, err)
			require.Equal(t, proto.V0, proto2.V0)
			require.Equal(t, proto.V1, proto2.V1)
			require.Equal(t, proto.V2, proto2.V2)
			require.Equal(t, proto.V3, proto2.V3)
		})
	}
}

func TestUint256_MaxValue(t *testing.T) {
	t.Parallel()

	// 2^256 - 1
	maxU256 := new(uint256.Int).Sub(new(uint256.Int).Lsh(uint256.NewInt(1), 256), uint256.NewInt(1))
	proto := NewUint256(maxU256)

	var dst uint256.Int
	proto.IntoUint256(&dst)
	require.True(t, maxU256.Eq(&dst))

	// Verify ToBigInt round-trips correctly for max value
	big := proto.ToBigInt()
	require.Equal(t, maxU256.Dec(), big.String())
}

func TestUint256_NilIntoUint256(t *testing.T) {
	t.Parallel()

	var dst uint256.Int
	dst.SetUint64(42) // set to non-zero first
	(*Uint256)(nil).IntoUint256(&dst)
	require.True(t, dst.IsZero(), "nil should clear dst to zero")
}
