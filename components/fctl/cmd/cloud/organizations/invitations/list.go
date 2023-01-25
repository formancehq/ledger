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
		statusFlag = "status"
	)
	return fctl.NewCommand("list",
		fctl.WithAliases("ls", "l"),
		fctl.WithArgs(cobra.ExactArgs(0)),
		fctl.WithShortDescription("List invitations"),
		fctl.WithAliases("s"),
		fctl.WithStringFlag(statusFlag, "", "Filter invitations by status"),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}

			apiClient, err := fctl.NewMembershipClient(cmd, cfg)
			if err != nil {
				return err
			}

			organizationID, err := fctl.ResolveOrganizationID(cmd, cfg)
			if err != nil {
				return err
			}

			listInvitationsResponse, _, err := apiClient.DefaultApi.
				ListOrganizationInvitations(cmd.Context(), organizationID).
				Status(fctl.GetString(cmd, statusFlag)).
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
			tableData = fctl.Prepend(tableData, []string{"ID", "Email", "Status", "Creation date"})
			return pterm.DefaultTable.
				WithHasHeader().
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
