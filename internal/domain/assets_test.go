package domain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAssetPrecision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		asset    string
		wantBase string
		wantPrec uint8
	}{
		{"USD/4", "USD", 4},
		{"EUR/2", "EUR", 2},
		{"BTC/8", "BTC", 8},
		{"USD", "USD", 0},
		{"ABCDEFGHIJKLMNOPQ/6", "ABCDEFGHIJKLMNOPQ", 6},
	}

	for _, tt := range tests {
		t.Run(tt.asset, func(t *testing.T) {
			t.Parallel()

			base, prec := ParseAssetPrecision(tt.asset)
			require.Equal(t, tt.wantBase, base)
			require.Equal(t, tt.wantPrec, prec)
		})
	}
}

func TestFormatAsset(t *testing.T) {
	t.Parallel()

	require.Equal(t, "USD/4", FormatAsset("USD", 4))
	require.Equal(t, "EUR", FormatAsset("EUR", 0))
	require.Equal(t, "BTC/8", FormatAsset("BTC", 8))
}
