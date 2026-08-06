package transactions

import (
	"github.com/pterm/pterm"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// renderPostCommitVolumes displays a PostCommitVolumes table in the CLI output.
// Volumes are listed per (account, asset, color). The "" color is rendered as
// "-" so the uncolored bucket stands out in the table.
func renderPostCommitVolumes(pcv *commonpb.PostCommitVolumes) error {
	if len(pcv.GetVolumes()) == 0 {
		return nil
	}

	pterm.Println()
	pterm.Println("Post-Commit Volumes:")

	table := pterm.TableData{
		{"ACCOUNT", "ASSET", "COLOR", "INPUT", "OUTPUT"},
	}

	// Rows are sorted by (account, asset, color) server-side.
	for _, row := range pcv.GetVolumes() {
		displayColor := row.GetColor()
		if displayColor == "" {
			displayColor = "-"
		}
		table = append(table, []string{
			row.GetAccount(),
			row.GetAsset(),
			displayColor,
			row.GetInput(),
			row.GetOutput(),
		})
	}

	return pterm.DefaultTable.WithHasHeader().WithData(table).Render()
}
