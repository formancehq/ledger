package organizations

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewDeleteCommand() *cobra.Command {
	return fctl.NewCommand("delete <organization-id>",
		fctl.WithAliases("del", "d"),
		fctl.WithShortDescription("Delete organization"),
		fctl.WithArgs(cobra.ExactArgs(1)),
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

			if !fctl.CheckOrganizationApprobation(cmd, "You are about to delete an organization") {
				return fctl.ErrMissingApproval
			}

			_, err = apiClient.DefaultApi.
				DeleteOrganization(cmd.Context(), args[0]).
				Execute()
			if err != nil {
				return err
			}

			fctl.Success(cmd.OutOrStdout(), "Organization '%s' deleted", args[0])

			return nil
		}),
	)
}
