package transactions

import (
	"sort"

	"github.com/pterm/pterm"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// renderPostCommitVolumes displays a PostCommitVolumes table in the CLI output.
// Volumes are listed per (account, asset, color). The "" color is rendered as
// "-" so the uncolored bucket stands out in the table.
func renderPostCommitVolumes(pcv *commonpb.PostCommitVolumes, rescale *uint8) error {
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

		// With --rescale, an account's volumes are aggregated by (base currency,
		// color) — USD/2 + USD/3 → one USD row per color — and re-expressed at
		// the requested scale, matching accounts get/aggregate-volumes. Otherwise
		// each entry is rendered raw.
		if rescale != nil {
			raw := make([]cmdutil.RawVolume, 0, len(vba.GetVolumes()))
			for _, entry := range vba.GetVolumes() {
				v := entry.GetVolumes()
				raw = append(raw, cmdutil.RawVolume{
					Asset:  entry.GetAsset(),
					Color:  entry.GetColor(),
					Input:  v.GetInput(),
					Output: v.GetOutput(),
				})
			}

			for _, av := range cmdutil.AggregateVolumes(raw) {
				displayColor := av.Color
				if displayColor == "" {
					displayColor = "-"
				}

				table = append(table, []string{
					account,
					invariants.FormatAsset(av.Asset, *rescale),
					displayColor,
					cmdutil.RescaleAmount(av.Input, av.Precision, *rescale),
					cmdutil.RescaleAmount(av.Output, av.Precision, *rescale),
				})
			}

			continue
		}

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
