package ledger

import (
	"fmt"

	"github.com/formancehq/fctl/cmd/ledger/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewStatsCommand() *cobra.Command {
	return fctl.NewCommand("stats",
		fctl.WithArgs(cobra.ExactArgs(0)),
		fctl.WithAliases("st"),
		fctl.WithShortDescription("Read ledger stats"),
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

			ledgerClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			response, _, err := ledgerClient.StatsApi.ReadStats(cmd.Context(), fctl.GetString(cmd, internal.LedgerFlag)).Execute()
			if err != nil {
				return err
			}

			tableData := pterm.TableData{}
			tableData = append(tableData, []string{pterm.LightCyan("Transactions"), fmt.Sprint(response.Data.Transactions)})
			tableData = append(tableData, []string{pterm.LightCyan("Accounts"), fmt.Sprint(response.Data.Accounts)})

			return pterm.DefaultTable.
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
