package accounts

import (
	"testing"

	"github.com/pterm/pterm"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestFormatAccountBalances(t *testing.T) {
	t.Parallel()

	t.Run("no volumes returns a single placeholder", func(t *testing.T) {
		t.Parallel()

		lines := formatAccountBalances(nil)
		if len(lines) != 1 {
			t.Fatalf("expected 1 placeholder line, got %d: %v", len(lines), lines)
		}

		if want := pterm.Gray("—"); lines[0] != want {
			t.Fatalf("expected placeholder %q, got %q", want, lines[0])
		}
	})

	t.Run("assets are sorted and colored by sign", func(t *testing.T) {
		t.Parallel()

		volumes := map[string]*commonpb.VolumesWithBalance{
			"USD/2": {Balance: "1000"},
			"EUR/2": {Balance: "-50"},
			"GBP/2": {Balance: "0"},
		}

		lines := formatAccountBalances(volumes)

		want := []string{
			"EUR/2 " + pterm.Red("-50"),
			"GBP/2 " + pterm.Green("0"),
			"USD/2 " + pterm.Green("1000"),
		}

		if len(lines) != len(want) {
			t.Fatalf("expected %d lines, got %d: %v", len(want), len(lines), lines)
		}

		for i := range want {
			if lines[i] != want[i] {
				t.Errorf("line %d: expected %q, got %q", i, want[i], lines[i])
			}
		}
	})
}
