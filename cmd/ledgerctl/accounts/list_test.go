package accounts

import (
	"testing"

	"github.com/pterm/pterm"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestFormatAccountBalances(t *testing.T) {
	t.Parallel()

	scale := func(u uint8) *uint8 { return &u }

	// The server returns Account.volumes already sorted by (asset, color)
	// ascending, so fixtures are written in that order.
	entry := func(asset, color, input, output, balance string) *commonpb.AccountVolume {
		return &commonpb.AccountVolume{
			Asset: asset,
			Color: color,
			Volumes: &commonpb.VolumesWithBalance{
				Input:   input,
				Output:  output,
				Balance: balance,
			},
		}
	}

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

	t.Run("without --rescale, assets keep server order and are colored by sign", func(t *testing.T) {
		t.Parallel()

		volumes := []*commonpb.AccountVolume{
			entry("EUR/2", "", "", "", "-50"),
			entry("GBP/2", "", "", "", "0"),
			entry("USD/2", "", "", "", "1000"),
		}

		assertLines(t, formatAccountBalances(volumes, nil), []string{
			"EUR/2 " + pterm.Red("-50"),
			"GBP/2 " + pterm.Green("0"),
			"USD/2 " + pterm.Green("1000"),
		})
	})

	t.Run("colored buckets are labelled and stay segregated", func(t *testing.T) {
		t.Parallel()

		volumes := []*commonpb.AccountVolume{
			entry("USD/2", "", "1000", "0", "1000"),
			entry("USD/2", "GREEN", "0", "250", "-250"),
		}

		assertLines(t, formatAccountBalances(volumes, nil), []string{
			"USD/2 " + pterm.Green("1000"),
			"USD/2[GREEN] " + pterm.Red("-250"),
		})

		assertLines(t, formatAccountBalances(volumes, scale(0)), []string{
			"USD " + pterm.Green("10.00"),
			"USD[GREEN] " + pterm.Red("-2.50"),
		})
	})

	t.Run("rescale to scale 0 sums currencies that differ only in precision", func(t *testing.T) {
		t.Parallel()

		volumes := []*commonpb.AccountVolume{
			entry("EUR/2", "", "250", "0", "250"),             // 2.50
			entry("USD/4", "", "10000", "0", "10000"),         // 1.0000
			entry("USD/8", "", "100000000", "0", "100000000"), // 1.00000000
		}

		assertLines(t, formatAccountBalances(volumes, scale(0)), []string{
			"EUR " + pterm.Green("2.50"),
			"USD " + pterm.Green("2.00000000"), // summed at the highest precision (8)
		})
	})

	t.Run("rescale to scale 0 divides by precision and drops the suffix", func(t *testing.T) {
		t.Parallel()

		volumes := []*commonpb.AccountVolume{
			entry("EUR/2", "", "0", "50", "-50"),
			entry("JPY", "", "1000", "0", "1000"),
			entry("USD/3", "", "1123456780", "0", "1123456780"),
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
		volumes := []*commonpb.AccountVolume{
			entry("USD/2", "", "1234", "0", "1234"),
			entry("USD/3", "", "56789", "0", "56789"),
		}

		assertLines(t, formatAccountBalances(volumes, scale(2)), []string{
			"USD/2 " + pterm.Green("6912.9"),
		})
	})
}
