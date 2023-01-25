package connectors

import (
	"github.com/formancehq/fctl/cmd/payments/connectors/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/spf13/cobra"
)

func NewUninstallCommand() *cobra.Command {
	return fctl.NewCommand("uninstall <connector-name>",
		fctl.WithAliases("uninstall", "u", "un"),
		fctl.WithConfirmFlag(),
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithValidArgs(internal.AllConnectors...),
		fctl.WithShortDescription("Uninstall a connector"),
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

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to uninstall connector '%s'", args[0]) {
				return fctl.ErrMissingApproval
			}

			client, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			_, err = client.PaymentsApi.UninstallConnector(cmd.Context(), formance.Connector(args[0])).Execute()
			if err != nil {
				return fctl.WrapError(err, "uninstalling connector")
			}
			fctl.Success(cmd.OutOrStdout(), "Connector '%s' uninstalled!", args[0])
			return nil
		}),
	)
}
