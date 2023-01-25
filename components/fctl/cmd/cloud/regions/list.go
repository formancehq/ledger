package regions

import (
	"github.com/formancehq/fctl/membershipclient"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewListCommand() *cobra.Command {
	return fctl.NewCommand("list",
		fctl.WithArgs(cobra.ExactArgs(0)),
		fctl.WithAliases("ls", "l"),
		fctl.WithShortDescription("List users"),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}

			apiClient, err := fctl.NewMembershipClient(cmd, cfg)
			if err != nil {
				return err
			}

			regionsResponse, _, err := apiClient.DefaultApi.ListRegions(cmd.Context()).Execute()
			if err != nil {
				return err
			}

			tableData := fctl.Map(regionsResponse.Data, func(i membershipclient.Region) []string {
				return []string{
					i.Id,
					i.BaseUrl,
				}
			})
			tableData = fctl.Prepend(tableData, []string{"ID", "Base url"})
			return pterm.DefaultTable.
				WithHasHeader().
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
