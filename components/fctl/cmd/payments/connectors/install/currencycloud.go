package install

import (
	"github.com/formancehq/fctl/cmd/payments/connectors/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewCurrencyCloudCommand() *cobra.Command {
	const (
		endpointFlag    = "endpoint"
		defaultEndpoint = "https://devapi.currencycloud.com"
	)
	return fctl.NewCommand(internal.CurrencyCloudConnector+" <login-id> <api-key>",
		fctl.WithShortDescription("Install a Currency Cloud connector"),
		fctl.WithArgs(cobra.ExactArgs(2)),
		fctl.WithStringFlag(endpointFlag, defaultEndpoint, "API endpoint"),
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

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to install connector '%s'", internal.CurrencyCloudConnector) {
				return fctl.ErrMissingApproval
			}

			paymentsClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			var endpoint *string
			if e := fctl.GetString(cmd, endpointFlag); e != "" {
				endpoint = &e
			}

			_, err = paymentsClient.PaymentsApi.InstallConnector(cmd.Context(), internal.CurrencyCloudConnector).
				ConnectorConfig(formance.ConnectorConfig{
					CurrencyCloudConfig: &formance.CurrencyCloudConfig{
						ApiKey:   args[1],
						LoginID:  args[0],
						Endpoint: endpoint,
					},
				}).
				Execute()

			fctl.Success(cmd.OutOrStdout(), "Connector installed!")

			return errors.Wrap(err, "installing connector")
		}),
	)
}
