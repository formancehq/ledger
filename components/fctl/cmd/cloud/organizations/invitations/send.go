package invitations

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewSendCommand() *cobra.Command {
	return fctl.NewCommand("send <email>",
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithShortDescription("Invite a user by email"),
		fctl.WithAliases("s"),
		fctl.WithConfirmFlag(),
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

			if !fctl.CheckOrganizationApprobation(cmd, "You are about to send an invitation") {
				return fctl.ErrMissingApproval
			}

			_, _, err = apiClient.DefaultApi.
				CreateInvitation(cmd.Context(), organizationID).
				Email(args[0]).
				Execute()
			if err != nil {
				return err
			}

			fctl.Success(cmd.OutOrStdout(), "Invitation sent")
			return nil
		}),
	)
}
