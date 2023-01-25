package accounts

import (
	internal "github.com/formancehq/fctl/cmd/ledger/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewListCommand() *cobra.Command {
	const (
		metadataFlag = "metadata"
	)
	return fctl.NewCommand("list",
		fctl.WithAliases("ls", "l"),
		fctl.WithShortDescription("List accounts"),
		fctl.WithArgs(cobra.ExactArgs(0)),
		fctl.WithStringSliceFlag(metadataFlag, []string{}, "Filter accounts with metadata"),
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

			metadata, err := fctl.ParseMetadata(fctl.GetStringSlice(cmd, metadataFlag))
			if err != nil {
				return err
			}

			ledger := fctl.GetString(cmd, internal.LedgerFlag)
			rsp, _, err := ledgerClient.AccountsApi.
				ListAccounts(cmd.Context(), ledger).
				Metadata(metadata).
				Execute()
			if err != nil {
				return err
			}

			tableData := fctl.Map(rsp.Cursor.Data, func(account formance.Account) []string {
				return []string{
					account.Address,
					fctl.MetadataAsShortString(account.Metadata),
				}
			})
			tableData = fctl.Prepend(tableData, []string{"Address", "Metadata"})
			return pterm.DefaultTable.
				WithHasHeader().
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
