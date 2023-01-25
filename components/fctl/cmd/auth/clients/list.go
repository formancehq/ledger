package clients

import (
	"strings"

	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewListCommand() *cobra.Command {
	return fctl.NewCommand("list",
		fctl.WithAliases("ls", "l"),
		fctl.WithArgs(cobra.ExactArgs(0)),
		fctl.WithShortDescription("List clients"),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}

			organizationID, err := fctl.ResolveOrganizationID(cmd, cfg)
			if err != nil {
				return err
			}

			stack, err := fctl.ResolveStack(cmd, cfg, organizationID)
			if err != nil {
				return err
			}

			authClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			clients, _, err := authClient.ClientsApi.ListClients(cmd.Context()).Execute()
			if err != nil {
				return err
			}

			tableData := fctl.Map(clients.Data, func(o formance.Client) []string {
				return []string{
					o.Id,
					o.Name,
					func() string {
						if o.Description == nil {
							return ""
						}
						return ""
					}(),
					strings.Join(o.Scopes, ","),
					fctl.BoolPointerToString(o.Public),
				}
			})
			tableData = fctl.Prepend(tableData, []string{"ID", "Name", "Description", "Scopes", "Public"})
			return pterm.DefaultTable.
				WithHasHeader().
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
