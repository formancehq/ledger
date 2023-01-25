package clients

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewDeleteCommand() *cobra.Command {
	return fctl.NewCommand("delete <client-id>",
		fctl.WithConfirmFlag(),
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithAliases("d", "del"),
		fctl.WithShortDescription("Delete client"),
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

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to delete an OAuth2 client") {
				return fctl.ErrMissingApproval
			}

			authClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			_, err = authClient.ClientsApi.DeleteClient(cmd.Context(), args[0]).Execute()
			if err != nil {
				return err
			}

			fctl.Success(cmd.OutOrStdout(), "Client deleted!")
			return nil
		}),
	)
}
