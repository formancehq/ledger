package ledger

import (
	"fmt"

	internal "github.com/formancehq/fctl/cmd/ledger/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewBalancesCommand() *cobra.Command {
	const (
		afterFlag   = "after"
		addressFlag = "address"
	)
	return fctl.NewCommand("balances",
		fctl.WithAliases("balance", "bal", "b"),
		fctl.WithStringFlag(addressFlag, "", "Filter on specific address"),
		fctl.WithStringFlag(afterFlag, "", "Filter after specific address"),
		fctl.WithShortDescription("Read balances"),
		fctl.WithArgs(cobra.ExactArgs(0)),
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

			client, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			balances, _, err := client.BalancesApi.
				GetBalances(cmd.Context(), fctl.GetString(cmd, internal.LedgerFlag)).
				After(fctl.GetString(cmd, afterFlag)).
				Address(fctl.GetString(cmd, addressFlag)).
				Execute()
			if err != nil {
				return err
			}

			tableData := pterm.TableData{}
			tableData = append(tableData, []string{"Account", "Asset", "Balance"})
			for _, accountBalances := range balances.Cursor.Data {
				for account, volumes := range accountBalances {
					for asset, balance := range volumes {
						tableData = append(tableData, []string{
							account, asset, fmt.Sprint(balance),
						})
					}
				}
			}
			if err := pterm.DefaultTable.
				WithHasHeader(true).
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render(); err != nil {
				return err
			}

			return nil
		}),
	)
}
