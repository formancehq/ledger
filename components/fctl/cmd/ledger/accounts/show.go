package accounts

import (
	"fmt"

	"github.com/formancehq/fctl/cmd/ledger/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewShowCommand() *cobra.Command {
	return fctl.NewCommand("show <address>",
		fctl.WithShortDescription("Show account"),
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithAliases("sh", "s"),
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

			ledger := fctl.GetString(cmd, internal.LedgerFlag)
			rsp, _, err := ledgerClient.AccountsApi.GetAccount(cmd.Context(), ledger, args[0]).Execute()
			if err != nil {
				return err
			}

			fctl.Section.WithWriter(cmd.OutOrStdout()).Println("Information")
			if rsp.Data.Volumes != nil && len(*rsp.Data.Volumes) > 0 {
				tableData := pterm.TableData{}
				tableData = append(tableData, []string{"Asset", "Input", "Output"})
				for asset, volumes := range *rsp.Data.Volumes {
					input := volumes["input"]
					output := volumes["output"]
					tableData = append(tableData, []string{pterm.LightCyan(asset), fmt.Sprint(input), fmt.Sprint(output)})
				}
				if err := pterm.DefaultTable.
					WithHasHeader(true).
					WithWriter(cmd.OutOrStdout()).
					WithData(tableData).
					Render(); err != nil {
					return err
				}
			} else {
				fctl.Println("No balances.")
			}

			fmt.Fprintln(cmd.OutOrStdout())

			if err := fctl.PrintMetadata(cmd.OutOrStdout(), rsp.Data.Metadata); err != nil {
				return err
			}

			return nil
		}),
	)
}
