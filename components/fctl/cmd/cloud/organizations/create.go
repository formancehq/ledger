package organizations

import (
	"github.com/formancehq/fctl/membershipclient"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCreateCommand() *cobra.Command {
	return fctl.NewCommand("create <name>",
		fctl.WithAliases("cr", "c"),
		fctl.WithShortDescription("Create organization"),
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

			if !fctl.CheckOrganizationApprobation(cmd, "You are about to create a new organization") {
				return fctl.ErrMissingApproval
			}

			response, _, err := apiClient.DefaultApi.
				CreateOrganization(cmd.Context()).
				Body(membershipclient.OrganizationData{
					Name: args[0],
				}).Execute()
			if err != nil {
				return err
			}

			fctl.Success(cmd.OutOrStdout(), "Organization '%s' created with ID: %s", args[0], response.Data.Id)

			return nil
		}),
	)
}
