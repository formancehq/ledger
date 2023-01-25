package invitations

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewAcceptCommand() *cobra.Command {
	return fctl.NewCommand("accept <invitation-id>",
		fctl.WithAliases("a"),
		fctl.WithShortDescription("Accept invitation"),
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

			if !fctl.CheckOrganizationApprobation(cmd, "You are about to accept an invitation") {
				return fctl.ErrMissingApproval
			}

			_, err = client.DefaultApi.AcceptInvitation(cmd.Context(), args[0]).Execute()
			if err != nil {
				return err
			}

			fctl.Success(cmd.OutOrStdout(), "Invitation accepted!")
			return nil
		}),
	)
}
