package invitations

import (
	"time"

	"github.com/formancehq/fctl/membershipclient"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewListCommand() *cobra.Command {
	const (
		statusFlag       = "status"
		organizationFlag = "organization"
	)
	return fctl.NewCommand("list",
		fctl.WithAliases("ls", "l"),
		fctl.WithShortDescription("List invitations"),
		fctl.WithStringFlag(statusFlag, "", "Filter invitations by status"),
		fctl.WithArgs(cobra.ExactArgs(0)),
		fctl.WithStringFlag(organizationFlag, "", "Filter invitations by organization"),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}
			client, err := fctl.NewMembershipClient(cmd, cfg)
			if err != nil {
				return err
			}

			listInvitationsResponse, _, err := client.DefaultApi.
				ListInvitations(cmd.Context()).
				Status(fctl.GetString(cmd, statusFlag)).
				Organization(fctl.GetString(cmd, organizationFlag)).
				Execute()
			if err != nil {
				return err
			}

			tableData := fctl.Map(listInvitationsResponse.Data, func(i membershipclient.Invitation) []string {
				return []string{
					i.Id,
					i.UserEmail,
					i.Status,
					i.CreationDate.Format(time.RFC3339),
				}
			})
			tableData = fctl.Prepend(tableData, []string{"ID", "Email", "Status", "CreationDate"})
			return pterm.DefaultTable.
				WithHasHeader().
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
