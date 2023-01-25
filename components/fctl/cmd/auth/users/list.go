package users

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewListCommand() *cobra.Command {
	return fctl.NewCommand("list",
		fctl.WithAliases("ls", "l"),
		fctl.WithShortDescription("List users"),
		fctl.WithArgs(cobra.ExactArgs(0)),
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

			client, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			listUsersResponse, _, err := client.UsersApi.ListUsers(cmd.Context()).Execute()
			if err != nil {
				return err
			}

			if len(listUsersResponse.Data) == 0 {
				fctl.Println("No users found.")
				return nil
			}

			tableData := fctl.Map(listUsersResponse.Data, func(o formance.User) []string {
				return []string{
					*o.Id,
					*o.Subject,
					*o.Email,
				}
			})
			tableData = fctl.Prepend(tableData, []string{"ID", "Subject", "Email"})
			return pterm.DefaultTable.
				WithHasHeader().
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
