package transactions

import (
	"sort"

	"github.com/pterm/pterm"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// renderPostCommitVolumes displays a PostCommitVolumes table in the CLI output.
func renderPostCommitVolumes(pcv *commonpb.PostCommitVolumes) error {
	if len(pcv.GetVolumesByAccount()) == 0 {
		return nil
	}

	pterm.Println()
	pterm.Println("Post-Commit Volumes:")

	table := pterm.TableData{
		{"ACCOUNT", "ASSET", "INPUT", "OUTPUT"},
	}

	// Sort accounts for stable output
	accounts := make([]string, 0, len(pcv.GetVolumesByAccount()))
	for account := range pcv.GetVolumesByAccount() {
		accounts = append(accounts, account)
	}

	sort.Strings(accounts)

	for _, account := range accounts {
		vba := pcv.GetVolumesByAccount()[account]

		// Sort assets for stable output
		assets := make([]string, 0, len(vba.GetVolumes()))
		for asset := range vba.GetVolumes() {
			assets = append(assets, asset)
		}

		sort.Strings(assets)

		for _, asset := range assets {
			v := vba.GetVolumes()[asset]
			table = append(table, []string{
				account,
				asset,
				v.GetInput(),
				v.GetOutput(),
			})
		}
	}

	return pterm.DefaultTable.WithHasHeader().WithData(table).Render()
}
