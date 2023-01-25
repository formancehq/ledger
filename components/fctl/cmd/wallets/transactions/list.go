package transactions

import (
	"fmt"
	"time"

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
		fctl.WithShortDescription("List transactions"),
		fctl.WithArgs(cobra.ExactArgs(0)),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return errors.Wrap(err, "retriecing config")
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

			walletID, err := internal.RetrieveWalletID(cmd, client)
			if err != nil {
				return err
			}

			res, _, err := client.WalletsApi.GetTransactions(cmd.Context()).WalletId(walletID).Execute()
			if err != nil {
				return errors.Wrap(err, "listing wallets")
			}

			if len(res.Cursor.Data) == 0 {
				fctl.Println("No transactions found.")
				return nil
			}

			tableData := fctl.Map(res.Cursor.Data, func(tx formance.WalletsTransaction) []string {
				return []string{
					fmt.Sprintf("%d", tx.Txid),
					tx.Timestamp.Format(time.RFC3339),
					fctl.MetadataAsShortString(tx.Metadata),
				}
			})
			tableData = fctl.Prepend(tableData, []string{"ID", "Date", "Metadata"})
			return pterm.DefaultTable.
				WithHasHeader().
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
