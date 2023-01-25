package install

import (
	"github.com/formancehq/fctl/cmd/payments/connectors/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewWiseCommand() *cobra.Command {
	return fctl.NewCommand(internal.WiseConnector+" <api-key>",
		fctl.WithShortDescription("Install a Wise connector"),
		fctl.WithArgs(cobra.ExactArgs(1)),
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

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to install connector '%s'", internal.WiseConnector) {
				return fctl.ErrMissingApproval
			}

			paymentsClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			_, err = paymentsClient.PaymentsApi.InstallConnector(cmd.Context(), internal.WiseConnector).
				ConnectorConfig(formance.ConnectorConfig{
					WiseConfig: &formance.WiseConfig{
						ApiKey: args[1],
					},
				}).
				Execute()

			fctl.Success(cmd.OutOrStdout(), "Connector installed!")

			return errors.Wrap(err, "installing connector")
		}),
	)
}
