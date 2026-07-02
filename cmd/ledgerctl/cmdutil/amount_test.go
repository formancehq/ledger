package cmdutil

import (
	"math/big"
	"testing"
)

func TestRescale(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		amount     string
		asset      string
		toScale    uint8
		wantAmount string
		wantAsset  string
	}{
		{
			name:       "coarser scale exposes a fractional part",
			amount:     "1234",
			asset:      "USD/2",
			toScale:    0,
			wantAmount: "12.34",
			wantAsset:  "USD",
		},
		{
			name:       "same scale is a no-op that keeps the suffix",
			amount:     "1234",
			asset:      "USD/2",
			toScale:    2,
			wantAmount: "1234",
			wantAsset:  "USD/2",
		},
		{
			name:       "finer scale pads with zeros",
			amount:     "1234",
			asset:      "USD/2",
			toScale:    4,
			wantAmount: "123400",
			wantAsset:  "USD/4",
		},
		{
			name:       "bare currency, scale 0",
			amount:     "1000",
			asset:      "JPY",
			toScale:    0,
			wantAmount: "1000",
			wantAsset:  "JPY",
		},
		{
			name:       "negative amount keeps its sign",
			amount:     "-50",
			asset:      "EUR/2",
			toScale:    0,
			wantAmount: "-0.50",
			wantAsset:  "EUR",
		},
		{
			name:       "unparseable amount is left as-is",
			amount:     "not-a-number",
			asset:      "USD/2",
			toScale:    0,
			wantAmount: "not-a-number",
			wantAsset:  "USD/2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotAmount, gotAsset := Rescale(tc.amount, tc.asset, tc.toScale)
			if gotAmount != tc.wantAmount || gotAsset != tc.wantAsset {
				t.Errorf("Rescale(%q, %q, %d) = (%q, %q), want (%q, %q)",
					tc.amount, tc.asset, tc.toScale, gotAmount, gotAsset, tc.wantAmount, tc.wantAsset)
			}
		})
	}
}

func TestRescaleAmount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		amount        int64
		fromPrecision uint8
		toScale       uint8
		want          string
	}{
		{"coarser", 69129, 3, 0, "69.129"},
		{"coarser by one", 69129, 3, 2, "6912.9"},
		{"same", 69129, 3, 3, "69129"},
		{"finer", 69129, 3, 5, "6912900"},
		{"precision zero", 1000, 0, 0, "1000"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := RescaleAmount(big.NewInt(tc.amount), tc.fromPrecision, tc.toScale); got != tc.want {
				t.Errorf("RescaleAmount(%d, %d, %d) = %q, want %q",
					tc.amount, tc.fromPrecision, tc.toScale, got, tc.want)
			}
		})
	}
}

func TestAggregateVolumesRescaleSpec(t *testing.T) {
	t.Parallel()

	// The spec example: 1234 USD/2 + 56789 USD/3, summed on real values.
	got := AggregateVolumes(map[string]RawVolume{
		"USD/2": {Input: "1234", Output: "0"},
		"USD/3": {Input: "56789", Output: "0"},
	})

	if len(got) != 1 {
		t.Fatalf("expected 1 aggregated currency, got %d: %+v", len(got), got)
	}

	av := got[0]

	// --rescale (scale 0) → 69.129 USD
	if bal, asset := RescaleAmount(av.Balance, av.Precision, 0), AssetLabel(av.Asset, 0); bal != "69.129" || asset != "USD" {
		t.Errorf("scale 0: got %s %s, want 69.129 USD", bal, asset)
	}

	// --rescale=2 → 6912.9 USD/2
	if bal, asset := RescaleAmount(av.Balance, av.Precision, 2), AssetLabel(av.Asset, 2); bal != "6912.9" || asset != "USD/2" {
		t.Errorf("scale 2: got %s %s, want 6912.9 USD/2", bal, asset)
	}
}

func TestAggregateVolumes(t *testing.T) {
	t.Parallel()

	type want struct {
		asset                  string
		precision              uint8
		input, output, balance string
	}

	// Render aggregated sums at their natural precision (scale 0 keeps every
	// fractional digit) for comparison.
	assert := func(t *testing.T, got []AssetVolumes, wants []want) {
		t.Helper()

		if len(got) != len(wants) {
			t.Fatalf("expected %d entries, got %d: %+v", len(wants), len(got), got)
		}

		for i, w := range wants {
			g := got[i]

			gotInput := RescaleAmount(g.Input, g.Precision, 0)
			gotOutput := RescaleAmount(g.Output, g.Precision, 0)
			gotBalance := RescaleAmount(g.Balance, g.Precision, 0)

			if g.Asset != w.asset || g.Precision != w.precision ||
				gotInput != w.input || gotOutput != w.output || gotBalance != w.balance {
				t.Errorf("entry %d: got {asset=%s prec=%d in=%s out=%s bal=%s}, want %+v",
					i, g.Asset, g.Precision, gotInput, gotOutput, gotBalance, w)
			}
		}
	}

	t.Run("sums input/output/balance across precisions at the highest precision", func(t *testing.T) {
		t.Parallel()

		got := AggregateVolumes(map[string]RawVolume{
			"USD/4": {Input: "10000", Output: "0"},     // in 1.0000
			"USD/8": {Input: "100000000", Output: "0"}, // in 1.00000000
			"EUR/2": {Input: "250", Output: "100"},     // in 2.50, out 1.00 → bal 1.50
		})

		assert(t, got, []want{
			{"EUR", 2, "2.50", "1.00", "1.50"},
			{"USD", 8, "2.00000000", "0.00000000", "2.00000000"},
		})
	})

	t.Run("balance is derived as input minus output and can be negative", func(t *testing.T) {
		t.Parallel()

		// 1.50 - 2.0000 = -0.5000 at precision 4.
		got := AggregateVolumes(map[string]RawVolume{
			"USD/2": {Input: "150", Output: "0"},
			"USD/4": {Input: "0", Output: "20000"},
		})

		assert(t, got, []want{
			{"USD", 4, "1.5000", "2.0000", "-0.5000"},
		})
	})

	t.Run("bare currency without precision", func(t *testing.T) {
		t.Parallel()

		got := AggregateVolumes(map[string]RawVolume{
			"JPY": {Input: "1000", Output: "400"},
		})

		assert(t, got, []want{
			{"JPY", 0, "1000", "400", "600"},
		})
	})
}
