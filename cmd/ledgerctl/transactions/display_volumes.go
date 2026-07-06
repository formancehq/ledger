package transactions

import (
	"sort"

	"github.com/pterm/pterm"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// renderPostCommitVolumes displays a PostCommitVolumes table in the CLI output.
func renderPostCommitVolumes(pcv *commonpb.PostCommitVolumes, rescale *uint8) error {
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

		// With --rescale, an account's per-asset volumes are aggregated by base
		// currency (USD/2 + USD/3 → one USD row) and re-expressed at the
		// requested scale, matching accounts get/aggregate-volumes. Otherwise
		// each asset is rendered raw, sorted for stable output.
		if rescale != nil {
			raw := make(map[string]cmdutil.RawVolume, len(vba.GetVolumes()))
			for asset, v := range vba.GetVolumes() {
				raw[asset] = cmdutil.RawVolume{Input: v.GetInput(), Output: v.GetOutput()}
			}

			for _, av := range cmdutil.AggregateVolumes(raw) {
				table = append(table, []string{
					account,
					invariants.FormatAsset(av.Asset, *rescale),
					cmdutil.RescaleAmount(av.Input, av.Precision, *rescale),
					cmdutil.RescaleAmount(av.Output, av.Precision, *rescale),
				})
			}

			continue
		}

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
