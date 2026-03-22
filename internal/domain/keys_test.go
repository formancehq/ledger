package domain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSinkConfigKey_Bytes(t *testing.T) {
	t.Parallel()

	k := SinkConfigKey{Name: "my-sink"}
	require.Equal(t, []byte("my-sink"), k.Bytes())
}

func TestVolumeKey_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		asset string
	}{
		{"with precision", "USD/4"},
		{"zero precision", "EUR"},
		{"high precision", "BTC/8"},
		{"underscore asset", "CUSTOM_TOKEN/2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vk := VolumeKey{
				AccountKey: AccountKey{Ledger: "ledger1", Account: "users:alice"},
				Asset:      tt.asset,
			}

			data := vk.Bytes()

			var decoded VolumeKey
			require.NoError(t, decoded.Unmarshal(data))
			require.Equal(t, vk, decoded)
		})
	}
}

func TestVolumeKey_ByteFormat(t *testing.T) {
	t.Parallel()

	vk := VolumeKey{
		AccountKey: AccountKey{Ledger: "l", Account: "a"},
		Asset:      "USD/4",
	}

	data := vk.Bytes()
	// Expected: "l" \x00 "a" \x00 "USD" \x03 \x04
	expected := []byte{'l', 0x00, 'a', 0x00, 'U', 'S', 'D', 0x03, 0x04}
	require.Equal(t, expected, data)
}
