package accounts

import (
	"testing"

	"github.com/pterm/pterm"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestFormatAccountBalances(t *testing.T) {
	t.Parallel()

	scale := func(u uint8) *uint8 { return &u }

	assertLines := func(t *testing.T, got, want []string) {
		t.Helper()

		if len(got) != len(want) {
			t.Fatalf("expected %d lines, got %d: %v", len(want), len(got), got)
		}

		for i := range want {
			if got[i] != want[i] {
				t.Errorf("line %d: expected %q, got %q", i, want[i], got[i])
			}
		}
	}

	t.Run("no volumes returns a single placeholder", func(t *testing.T) {
		t.Parallel()

		lines := formatAccountBalances(nil, nil)
		if len(lines) != 1 {
			t.Fatalf("expected 1 placeholder line, got %d: %v", len(lines), lines)
		}

		if want := pterm.Gray("—"); lines[0] != want {
			t.Fatalf("expected placeholder %q, got %q", want, lines[0])
		}
	})

	t.Run("without --rescale, assets are sorted and colored by sign", func(t *testing.T) {
		t.Parallel()

		volumes := map[string]*commonpb.VolumesWithBalance{
			"USD/2": {Balance: "1000"},
			"EUR/2": {Balance: "-50"},
			"GBP/2": {Balance: "0"},
		}

		assertLines(t, formatAccountBalances(volumes, nil), []string{
			"EUR/2 " + pterm.Red("-50"),
			"GBP/2 " + pterm.Green("0"),
			"USD/2 " + pterm.Green("1000"),
		})
	})

	t.Run("rescale to scale 0 sums currencies that differ only in precision", func(t *testing.T) {
		t.Parallel()

		volumes := map[string]*commonpb.VolumesWithBalance{
			"USD/4": {Input: "10000", Output: "0", Balance: "10000"},         // 1.0000
			"USD/8": {Input: "100000000", Output: "0", Balance: "100000000"}, // 1.00000000
			"EUR/2": {Input: "250", Output: "0", Balance: "250"},             // 2.50
		}

		assertLines(t, formatAccountBalances(volumes, scale(0)), []string{
			"EUR " + pterm.Green("2.50"),
			"USD " + pterm.Green("2.00000000"), // summed at the highest precision (8)
		})
	})

	t.Run("rescale to scale 0 divides by precision and drops the suffix", func(t *testing.T) {
		t.Parallel()

		volumes := map[string]*commonpb.VolumesWithBalance{
			"USD/3": {Input: "1123456780", Output: "0", Balance: "1123456780"},
			"EUR/2": {Input: "0", Output: "50", Balance: "-50"},
			"JPY":   {Input: "1000", Output: "0", Balance: "1000"},
		}

		assertLines(t, formatAccountBalances(volumes, scale(0)), []string{
			"EUR " + pterm.Red("-0.50"),
			"JPY " + pterm.Green("1000"),
			"USD " + pterm.Green("1123456.780"),
		})
	})

	t.Run("rescale to a non-zero scale keeps the suffix", func(t *testing.T) {
		t.Parallel()

		// 12.34 (USD/2) + 56.789 (USD/3) = 69.129 USD; at scale 2 → 6912.9 USD/2.
		volumes := map[string]*commonpb.VolumesWithBalance{
			"USD/2": {Input: "1234", Output: "0", Balance: "1234"},
			"USD/3": {Input: "56789", Output: "0", Balance: "56789"},
		}

		assertLines(t, formatAccountBalances(volumes, scale(2)), []string{
			"USD/2 " + pterm.Green("6912.9"),
		})
	})
}
