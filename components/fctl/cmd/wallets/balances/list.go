package balances

import (
	"github.com/formancehq/fctl/cmd/wallets/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewListCommand() *cobra.Command {
	return fctl.NewCommand("list",
		fctl.WithAliases("ls", "l"),
		fctl.WithShortDescription("List balances"),
		fctl.WithArgs(cobra.ExactArgs(0)),
		internal.WithTargetingWalletByName(),
		internal.WithTargetingWalletByID(),
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

			res, _, err := client.WalletsApi.ListBalances(cmd.Context(), walletID).Execute()
			if err != nil {
				return errors.Wrap(err, "listing balances")
			}

			if len(res.Cursor.Data) == 0 {
				fctl.Println("No balances found.")
				return nil
			}

			tableData := fctl.Map(res.Cursor.Data, func(balance formance.Balance) []string {
				return []string{
					balance.Name,
				}
			})
			tableData = fctl.Prepend(tableData, []string{"Name"})
			return pterm.DefaultTable.
				WithHasHeader().
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
