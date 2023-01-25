package profiles

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewListCommand() *cobra.Command {
	return fctl.NewCommand("list",
		fctl.WithAliases("l"),
		fctl.WithShortDescription("List profiles"),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {

			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}

			currentProfileName := fctl.GetCurrentProfileName(cmd, cfg)

			profiles := fctl.MapKeys(cfg.GetProfiles())
			tableData := fctl.Map(profiles, func(p string) []string {
				isCurrent := "No"
				if p == currentProfileName {
					isCurrent = "Yes"
				}
				return []string{p, isCurrent}
			})
			tableData = fctl.Prepend(tableData, []string{"Name", "Active"})
			return pterm.DefaultTable.
				WithHasHeader().
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}))
}
