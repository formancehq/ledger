package transactions

import (
	"sort"

	"github.com/pterm/pterm"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// renderPostCommitVolumes displays a PostCommitVolumes table in the CLI output.
// Volumes are listed per (account, asset, color). The "" color is rendered as
// "-" so the uncolored bucket stands out in the table.
func renderPostCommitVolumes(pcv *commonpb.PostCommitVolumes) error {
	if len(pcv.GetVolumesByAccount()) == 0 {
		return nil
	}

	pterm.Println()
	pterm.Println("Post-Commit Volumes:")

	table := pterm.TableData{
		{"ACCOUNT", "ASSET", "COLOR", "INPUT", "OUTPUT"},
	}

	accounts := make([]string, 0, len(pcv.GetVolumesByAccount()))
	for account := range pcv.GetVolumesByAccount() {
		accounts = append(accounts, account)
	}
	sort.Strings(accounts)

	for _, account := range accounts {
		vba := pcv.GetVolumesByAccount()[account]
		// VolumesByAssets.Volumes is sorted by (asset, color) server-side.
		for _, entry := range vba.GetVolumes() {
			v := entry.GetVolumes()
			displayColor := entry.GetColor()
			if displayColor == "" {
				displayColor = "-"
			}
			table = append(table, []string{
				account,
				entry.GetAsset(),
				displayColor,
				v.GetInput(),
				v.GetOutput(),
			})
		}
	}

	return pterm.DefaultTable.WithHasHeader().WithData(table).Render()
}
