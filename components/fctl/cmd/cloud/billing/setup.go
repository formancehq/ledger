package billing

import (
	"fmt"

	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewSetupCommand() *cobra.Command {
	return fctl.NewCommand("setup",
		fctl.WithAliases("s"),
		fctl.WithShortDescription("Create a new billing account"),
		fctl.WithArgs(cobra.ExactArgs(0)),
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

			billing, _, err := apiClient.DefaultApi.BillingSetup(cmd.Context(), organizationID).Execute()
			if err != nil {
				fctl.Error(cmd.OutOrStdout(), "You already have an active subscription")
				return nil
			}
			_ = fmt.Sprintf("Billing Portal: %s", billing.Data.Url)

			if err := fctl.Open(billing.Data.Url); err != nil {
				return err
			}

			fctl.Success(cmd.OutOrStdout(), "Billing Setup opened in your browser")
			return nil
		}),
	)
}
