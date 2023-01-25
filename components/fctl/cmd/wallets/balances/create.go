package balances

import (
	"github.com/formancehq/fctl/cmd/wallets/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewCreateCommand() *cobra.Command {
	return fctl.NewCommand("create <balance-name>",
		fctl.WithShortDescription("Create a new balance"),
		fctl.WithAliases("c", "cr"),
		fctl.WithConfirmFlag(),
		fctl.WithArgs(cobra.ExactArgs(1)),
		internal.WithTargetingWalletByID(),
		internal.WithTargetingWalletByName(),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return errors.Wrap(err, "retrieving config")
			}

			organizationID, err := fctl.ResolveOrganizationID(cmd, cfg)
			if err != nil {
				return err
			}

			stack, err := fctl.ResolveStack(cmd, cfg, organizationID)
			if err != nil {
				return err
			}

			client, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return errors.Wrap(err, "creating stack client")
			}

			walletID, err := internal.RequireWalletID(cmd, client)
			if err != nil {
				return err
			}

			res, _, err := client.WalletsApi.CreateBalance(cmd.Context(), walletID).Body(formance.Balance{
				Name: args[0],
			}).Execute()
			if err != nil {
				return errors.Wrap(err, "Creating wallets")
			}

			fctl.Success(cmd.OutOrStdout(),
				"Balance created successfully with name: %s", res.Data.Name)
			return nil
		}),
	)
}
