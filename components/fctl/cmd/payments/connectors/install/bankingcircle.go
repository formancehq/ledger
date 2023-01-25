package install

import (
	"github.com/formancehq/fctl/cmd/payments/connectors/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewBankingCircleCommand() *cobra.Command {
	const (
		endpointFlag              = "endpoint"
		authorizationEndpointFlag = "authorization-endpoint"

		defaultEndpoint              = "https://sandbox.bankingcircle.com"
		defaultAuthorizationEndpoint = "https://authorizationsandbox.bankingcircleconnect.com"
	)
	return fctl.NewCommand(internal.BankingCircleConnector+" <username> <password>",
		fctl.WithShortDescription("Install a Banking Circle connector"),
		fctl.WithArgs(cobra.ExactArgs(2)),
		fctl.WithStringFlag(endpointFlag, defaultEndpoint, "API endpoint"),
		fctl.WithStringFlag(authorizationEndpointFlag, defaultAuthorizationEndpoint, "Authorization endpoint"),
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

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to install connector '%s'", internal.BankingCircleConnector) {
				return fctl.ErrMissingApproval
			}

			paymentsClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			_, err = paymentsClient.PaymentsApi.InstallConnector(cmd.Context(), internal.BankingCircleConnector).
				ConnectorConfig(formance.ConnectorConfig{
					BankingCircleConfig: &formance.BankingCircleConfig{
						Username:              args[0],
						Password:              args[1],
						Endpoint:              fctl.GetString(cmd, endpointFlag),
						AuthorizationEndpoint: fctl.GetString(cmd, authorizationEndpointFlag),
					},
				}).
				Execute()

			fctl.Success(cmd.OutOrStdout(), "Connector installed!")

			return errors.Wrap(err, "installing connector")
		}),
	)
}
