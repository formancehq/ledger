package transactions

import (
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewGetCommand creates the transactions get command.
func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get [transaction-id]",
		Aliases: cmdutil.GetAliases,
		Short:   "Get a transaction by ID",
		Long: `Get detailed information about a transaction via gRPC.

If --ledger is not provided and only one ledger exists, it will be used automatically.
If multiple ledgers exist, you will be prompted to select one.

Examples:
  ledgerctl transactions get 42 --ledger my-ledger
  ledgerctl transactions get 42  # Will prompt for ledger if needed
  ledgerctl transactions get     # Will prompt for both ledger and transaction ID`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Uint64("checkpoint-id", 0, "Read from a query checkpoint instead of the live store")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

// RenderGet writes a transaction with the native response envelope and display options.
func RenderGet(cmd *cobra.Command, resp *servicepb.GetTransactionResponse) error {
	text := pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout())

	tx := resp.GetTransaction()

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	text.Println()

	// Display transaction header
	text.Printf("Transaction: %s\n", pterm.Cyan(fmt.Sprintf("#%d", tx.GetId())))
	text.Println(pterm.Gray("─────────────────────────────────"))

	// Display basic info
	if tx.GetReference() != "" {
		text.Printf("Reference:   %s\n", tx.GetReference())
	}

	if tx.GetTimestamp() != nil {
		text.Printf("Timestamp:   %s\n", pterm.Gray(tx.GetTimestamp().AsTime().Format("2006-01-02T15:04:05Z07:00")))
	}

	if tx.GetInsertedAt() != nil {
		text.Printf("Inserted At: %s\n", pterm.Gray(tx.GetInsertedAt().AsTime().Format("2006-01-02T15:04:05Z07:00")))
	}

	// Display reverted status
	if tx.GetReverted() {
		text.Printf("Reverted:    %s\n", pterm.Yellow("Yes"))

		if tx.GetRevertedAt() != nil {
			text.Printf("Reverted At: %s\n", pterm.Gray(tx.GetRevertedAt().AsTime().Format("2006-01-02T15:04:05Z07:00")))
		}
	} else {
		text.Printf("Reverted:    %s\n", pterm.Green("No"))
	}

	// Display postings
	if len(tx.GetPostings()) > 0 {
		text.Println()
		text.Println("Postings:")

		postingsTable := pterm.TableData{
			{"#", "SOURCE", "", "DESTINATION", "AMOUNT", "ASSET", "COLOR"},
		}

		termWidth := pterm.GetTerminalWidth()
		// Reserve space for #(3) + arrow(1) + AMOUNT(12) + ASSET(8) + separators(5*3=15) + indent(2)
		const fixedColsWidth = 3 + 1 + 12 + 8 + 15 + 2

		maxAddrWidth := max((termWidth-fixedColsWidth)/2, 15)

		const continuationIndent = "  "

		rescale := cmdutil.RescaleTarget(cmd)

		for i, posting := range tx.GetPostings() {
			srcLines := cmdutil.WrapText(posting.GetSource(), maxAddrWidth, ":")
			dstLines := cmdutil.WrapText(posting.GetDestination(), maxAddrWidth, ":")

			maxLines := max(len(dstLines), len(srcLines))

			for line := range maxLines {
				src, dst := "", ""
				num, arrow, amount, asset, color := "", "", "", "", ""

				if line < len(srcLines) {
					src = srcLines[line]
					if line > 0 {
						src = continuationIndent + src
					}
				}

				if line < len(dstLines) {
					dst = dstLines[line]
					if line > 0 {
						dst = continuationIndent + dst
					}
				}

				if line == 0 {
					num = strconv.Itoa(i + 1)
					arrow = "→"
					amount, asset = posting.GetAmount().Dec(), posting.GetAsset()

					if rescale != nil {
						var err error

						amount, asset, err = cmdutil.Rescale(amount, asset, *rescale)
						if err != nil {
							return err
						}
					}

					color = posting.GetColor()
					if color == "" {
						color = "-"
					}
				}

				postingsTable = append(postingsTable, []string{
					num, src, arrow, dst, amount, asset, color,
				})
			}
		}

		err := pterm.DefaultTable.WithWriter(cmd.OutOrStdout()).WithHasHeader().WithData(postingsTable).Render()
		if err != nil {
			return err
		}
	}

	// Display metadata
	if len(tx.GetMetadata()) > 0 {
		text.Println()
		text.Println("Metadata:")

		metadataTable := pterm.TableData{
			{"KEY", "VALUE"},
		}
		for key, value := range tx.GetMetadata() {
			metadataTable = append(metadataTable, []string{
				key,
				commonpb.MetadataValueToString(value),
			})
		}

		return pterm.DefaultTable.WithWriter(cmd.OutOrStdout()).WithHasHeader().WithData(metadataTable).Render()
	}

	return nil
}
