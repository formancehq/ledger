package billing

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewPortalCommand() *cobra.Command {
	return fctl.NewCommand("portal",
		fctl.WithAliases("p"),
		fctl.WithShortDescription("Access to Billing Portal"),
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

			billing, _, err := apiClient.DefaultApi.BillingPortal(cmd.Context(), organizationID).Execute()
			if err != nil {
				return err
			}

			if billing == nil {
				fctl.Error(cmd.OutOrStdout(), "Please subscribe to a plan to access Billing Portal")
				return nil
			}

			if err := fctl.Open(billing.Data.Url); err != nil {
				return err
			}

			fctl.Success(cmd.OutOrStdout(), "Billing Portal opened in your browser")
			return nil
		}),
	)
}
