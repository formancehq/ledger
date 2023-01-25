package invitations

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewDeclineCommand() *cobra.Command {
	return fctl.NewCommand("decline <invitation-id>",
		fctl.WithAliases("dec", "d"),
		fctl.WithShortDescription("Decline invitation"),
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithConfirmFlag(),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}

			client, err := fctl.NewMembershipClient(cmd, cfg)
			if err != nil {
				return err
			}

			if !fctl.CheckOrganizationApprobation(cmd, "You are about to decline an invitation") {
				return fctl.ErrMissingApproval
			}

			_, err = client.DefaultApi.DeclineInvitation(cmd.Context(), args[0]).Execute()
			if err != nil {
				return err
			}

			fctl.Success(cmd.OutOrStdout(), "Invitation declined!")
			return nil
		}),
	)
}
