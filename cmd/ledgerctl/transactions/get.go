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
		RunE:              runGet,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Uint64("checkpoint-id", 0, "Read from a query checkpoint instead of the live store")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runGet(cmd *cobra.Command, args []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	// Get ledger name (from flag or interactive selection)
	ledgerFlag, _ := cmd.Flags().GetString("ledger")

	ledgerName, err := cmdutil.SelectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

	// Get transaction ID (from args or prompt)
	var txID uint64
	if len(args) > 0 {
		txID, err = strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			pterm.Error.Printfln("Invalid transaction ID: %v", err)

			return cmdutil.Displayed(fmt.Errorf("invalid transaction ID: %w", err))
		}
	} else {
		input, err := pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter transaction ID").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		txID, err = strconv.ParseUint(input, 10, 64)
		if err != nil {
			pterm.Error.Printfln("Invalid transaction ID: %v", err)

			return cmdutil.Displayed(fmt.Errorf("invalid transaction ID: %w", err))
		}
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching transaction #%d...", txID))

	checkpointID, _ := cmd.Flags().GetUint64("checkpoint-id")

	resp, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledgerName,
		TransactionId: txID,
		CheckpointId:  checkpointID,
	})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get transaction", err)
	}

	_ = spinner.Stop()

	tx := resp.GetTransaction()

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	pterm.Println()

	// Display transaction header
	pterm.Printf("Transaction: %s\n", pterm.Cyan(fmt.Sprintf("#%d", tx.GetId())))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	// Display basic info
	if tx.GetReference() != "" {
		pterm.Printf("Reference:   %s\n", tx.GetReference())
	}

	if tx.GetTimestamp() != nil {
		pterm.Printf("Timestamp:   %s\n", pterm.Gray(tx.GetTimestamp().AsTime().Format("2006-01-02T15:04:05Z07:00")))
	}

	if tx.GetInsertedAt() != nil {
		pterm.Printf("Inserted At: %s\n", pterm.Gray(tx.GetInsertedAt().AsTime().Format("2006-01-02T15:04:05Z07:00")))
	}

	// Display reverted status
	if tx.GetReverted() {
		pterm.Printf("Reverted:    %s\n", pterm.Yellow("Yes"))

		if tx.GetRevertedAt() != nil {
			pterm.Printf("Reverted At: %s\n", pterm.Gray(tx.GetRevertedAt().AsTime().Format("2006-01-02T15:04:05Z07:00")))
		}
	} else {
		pterm.Printf("Reverted:    %s\n", pterm.Green("No"))
	}

	// Display receipt if available
	if resp.GetReceipt() != "" {
		pterm.Printf("Receipt:     %s\n", pterm.Gray(resp.GetReceipt()))
	}

	// Display postings
	if len(tx.GetPostings()) > 0 {
		pterm.Println()
		pterm.Println("Postings:")

		postingsTable := pterm.TableData{
			{"#", "SOURCE", "", "DESTINATION", "AMOUNT", "ASSET", "COLOR"},
		}

		termWidth := pterm.GetTerminalWidth()
		// Reserve space for #(3) + arrow(1) + AMOUNT(12) + ASSET(8) + separators(5*3=15) + indent(2)
		const fixedColsWidth = 3 + 1 + 12 + 8 + 15 + 2

		maxAddrWidth := max((termWidth-fixedColsWidth)/2, 15)

		const continuationIndent = "  "

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
					amount = posting.GetAmount().Dec()
					asset = posting.GetAsset()
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

		err := pterm.DefaultTable.WithHasHeader().WithData(postingsTable).Render()
		if err != nil {
			return err
		}
	}

	// Display metadata
	if len(tx.GetMetadata()) > 0 {
		pterm.Println()
		pterm.Println("Metadata:")

		metadataTable := pterm.TableData{
			{"KEY", "VALUE"},
		}
		for key, value := range tx.GetMetadata() {
			metadataTable = append(metadataTable, []string{
				key,
				commonpb.MetadataValueToString(value),
			})
		}

		return pterm.DefaultTable.WithHasHeader().WithData(metadataTable).Render()
	}

	return nil
}
