package domain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newVolumeKey(ak AccountKey, asset string) VolumeKey {
	base, prec := ParseAssetPrecision(asset)

	return VolumeKey{
		AccountKey:     ak,
		Asset:          asset,
		AssetBase:      base,
		AssetPrecision: prec,
	}
}

func TestSinkConfigKey_Bytes(t *testing.T) {
	t.Parallel()

	k := SinkConfigKey{Name: "my-sink"}
	require.Equal(t, []byte("my-sink"), k.Bytes())
}

func TestVolumeKey_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		asset    string
		wantBase string
		wantPrec uint8
	}{
		{"with precision", "USD/4", "USD", 4},
		{"zero precision", "EUR", "EUR", 0},
		{"high precision", "BTC/8", "BTC", 8},
		{"underscore asset", "CUSTOM_TOKEN/2", "CUSTOM_TOKEN", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vk := newVolumeKey(AccountKey{Ledger: "ledger1", Account: "users:alice"}, tt.asset)

			data := vk.Bytes()

			var decoded VolumeKey
			require.NoError(t, decoded.Unmarshal(data))
			require.Equal(t, vk, decoded)
			require.Equal(t, tt.asset, decoded.Asset)
			require.Equal(t, tt.wantBase, decoded.AssetBase)
			require.Equal(t, tt.wantPrec, decoded.AssetPrecision)
		})
	}
}

func TestVolumeKey_ByteFormat(t *testing.T) {
	t.Parallel()

	vk := newVolumeKey(AccountKey{Ledger: "l", Account: "a"}, "USD/4")

	data := vk.Bytes()
	// Expected: "l" \x00 "a" \xFD "USD" \x04
	expected := []byte{'l', 0x00, 'a', 0xFD, 'U', 'S', 'D', 0x04}
	require.Equal(t, expected, data)
}

func TestVolumeKey_StructLiteralFallback(t *testing.T) {
	t.Parallel()

	// VolumeKey constructed via struct literal (legacy pattern) should still work.
	vk := VolumeKey{
		AccountKey: AccountKey{Ledger: "l", Account: "a"},
		Asset:      "EUR/2",
	}

	data := vk.Bytes()
	expected := []byte{'l', 0x00, 'a', 0xFD, 'E', 'U', 'R', 0x02}
	require.Equal(t, expected, data)
}
