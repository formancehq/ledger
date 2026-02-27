package transactions

import (
	"sort"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/pterm/pterm"
)

// renderPostCommitVolumes displays a PostCommitVolumes table in the CLI output.
func renderPostCommitVolumes(pcv *commonpb.PostCommitVolumes) error {
	if len(pcv.VolumesByAccount) == 0 {
		return nil
	}

	pterm.Println()
	pterm.Println("Post-Commit Volumes:")

	table := pterm.TableData{
		{"ACCOUNT", "ASSET", "INPUT", "OUTPUT"},
	}

	// Sort accounts for stable output
	accounts := make([]string, 0, len(pcv.VolumesByAccount))
	for account := range pcv.VolumesByAccount {
		accounts = append(accounts, account)
	}
	sort.Strings(accounts)

	for _, account := range accounts {
		vba := pcv.VolumesByAccount[account]

		// Sort assets for stable output
		assets := make([]string, 0, len(vba.Volumes))
		for asset := range vba.Volumes {
			assets = append(assets, asset)
		}
		sort.Strings(assets)

		for _, asset := range assets {
			v := vba.Volumes[asset]
			table = append(table, []string{
				account,
				asset,
				v.Input,
				v.Output,
			})
		}
	}

	return pterm.DefaultTable.WithHasHeader().WithData(table).Render()
}
