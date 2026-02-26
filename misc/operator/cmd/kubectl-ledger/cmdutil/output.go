package cmdutil

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/pterm/pterm"
	"sigs.k8s.io/yaml"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

// PhaseColor returns a colored string for the given LedgerService phase.
func PhaseColor(phase string) string {
	switch phase {
	case "Running":
		return pterm.FgGreen.Sprint(phase)
	case "Degraded":
		return pterm.FgYellow.Sprint(phase)
	default:
		if phase == "" {
			phase = "Pending"
		}
		return pterm.FgGray.Sprint(phase)
	}
}

// RenderTable prints a pterm table with a header row.
func RenderTable(header []string, rows [][]string) {
	data := pterm.TableData{header}
	for _, row := range rows {
		data = append(data, row)
	}
	//nolint:errcheck // best-effort terminal rendering
	_ = pterm.DefaultTable.WithHasHeader().WithData(data).Render()
}

// OutputJSON writes v as indented JSON to stdout.
func OutputJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// OutputYAML writes v as YAML to stdout.
func OutputYAML(v any) error {
	b, err := yaml.Marshal(v)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(b)
	return err
}

// FormatAge formats a duration into a human-readable age string (e.g. "5d", "3h", "12m").
func FormatAge(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

// FormatReadyReplicas returns "ready/desired" for display.
func FormatReadyReplicas(ready int32, desired *int32) string {
	d := int32(3)
	if desired != nil {
		d = *desired
	}
	return fmt.Sprintf("%d/%d", ready, d)
}

// FormatImage returns "repository:tag" for the given image spec.
// When the image is inherited from LedgerDefaults and not set on the
// LedgerService itself, both fields are empty; show "<from defaults>" instead.
func FormatImage(img ledgerv1alpha1.ImageSpec) string {
	if img.Repository == "" && img.Tag == "" {
		return pterm.Gray("<from defaults>")
	}
	if img.Repository == "" {
		return ":" + img.Tag
	}
	if img.Tag == "" {
		return img.Repository
	}
	return fmt.Sprintf("%s:%s", img.Repository, img.Tag)
}

// RenderBoxedTable prints a boxed key-value table (no header).
func RenderBoxedTable(rows [][]string) {
	_ = pterm.DefaultTable.
		WithHasHeader(false).
		WithBoxed(true).
		WithData(rows).
		Render()
}

// Separator prints a gray separator line.
func Separator() {
	pterm.Println(pterm.Gray("─────────────────────────────────────────"))
}
