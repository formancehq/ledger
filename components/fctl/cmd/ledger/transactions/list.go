package transactions

import (
	"fmt"
	"time"

	internal "github.com/formancehq/fctl/cmd/ledger/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewListCommand() *cobra.Command {
	const (
		pageSizeFlag    = "page-size"
		metadataFlag    = "metadata"
		referenceFlag   = "reference"
		accountFlag     = "account"
		destinationFlag = "dst"
		sourceFlag      = "src"
		afterFlag       = "after"
		endTimeFlag     = "end"
		startTimeFlag   = "start"
	)

	return fctl.NewCommand("list",
		fctl.WithAliases("ls", "l"),
		fctl.WithShortDescription("List transactions"),
		fctl.WithStringFlag(accountFlag, "", "Filter on account"),
		fctl.WithStringFlag(destinationFlag, "", "Filter on destination account"),
		fctl.WithStringFlag(afterFlag, "", "Filter results after given tx id"),
		fctl.WithStringFlag(endTimeFlag, "", "Consider transactions before date"),
		fctl.WithStringFlag(startTimeFlag, "", "Consider transactions after date"),
		fctl.WithStringFlag(sourceFlag, "", "Filter on source account"),
		fctl.WithStringFlag(referenceFlag, "", "Filter on reference"),
		fctl.WithStringSliceFlag(metadataFlag, []string{}, "Filter transactions with metadata"),
		fctl.WithIntFlag(pageSizeFlag, 5, "Page size"),
		// SDK not generating correct requests
		fctl.WithHiddenFlag(metadataFlag),
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

			ledgerClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			metadata, err := fctl.ParseMetadata(fctl.GetStringSlice(cmd, metadataFlag))
			if err != nil {
				return err
			}

			var (
				endTime   time.Time
				startTime time.Time
			)
			if startTimeStr := fctl.GetString(cmd, startTimeFlag); startTimeStr != "" {
				startTime, err = time.Parse(time.RFC3339Nano, startTimeStr)
				if err != nil {
					return err
				}
			}
			if endTimeStr := fctl.GetString(cmd, endTimeFlag); endTimeStr != "" {
				endTime, err = time.Parse(time.RFC3339Nano, endTimeStr)
				if err != nil {
					return err
				}
			}

			ledger := fctl.GetString(cmd, internal.LedgerFlag)
			rsp, _, err := ledgerClient.TransactionsApi.
				ListTransactions(cmd.Context(), ledger).
				PageSize(int64(fctl.GetInt(cmd, pageSizeFlag))).
				Reference(fctl.GetString(cmd, referenceFlag)).
				Account(fctl.GetString(cmd, accountFlag)).
				Destination(fctl.GetString(cmd, destinationFlag)).
				Source(fctl.GetString(cmd, sourceFlag)).
				After(fctl.GetString(cmd, afterFlag)).
				EndTime(endTime).
				StartTime(startTime).
				Metadata(metadata).
				Execute()
			if err != nil {
				return err
			}

			if len(rsp.Cursor.Data) == 0 {
				fctl.Println("No transactions found.")
				return nil
			}

			tableData := fctl.Map(rsp.Cursor.Data, func(tx formance.Transaction) []string {
				return []string{
					fmt.Sprintf("%d", tx.Txid),
					func() string {
						if tx.Reference == nil {
							return ""
						}
						return *tx.Reference
					}(),
					tx.Timestamp.Format(time.RFC3339),
					fctl.MetadataAsShortString(tx.Metadata),
				}
			})
			tableData = fctl.Prepend(tableData, []string{"ID", "Reference", "Date", "Metadata"})
			return pterm.DefaultTable.
				WithHasHeader().
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
