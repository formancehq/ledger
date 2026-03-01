package transactions

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewGetCommand creates the transactions get command.
func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get [transaction-id]",
		Aliases: []string{"g", "show", "describe"},
		Short:   "Get a transaction by ID",
		Long: `Get detailed information about a transaction via gRPC.

If --ledger is not provided and only one ledger exists, it will be used automatically.
If multiple ledgers exist, you will be prompted to select one.

Examples:
  ledgerctl transactions get 42 --ledger my-ledger
  ledgerctl transactions get 42  # Will prompt for ledger if needed
  ledgerctl transactions get     # Will prompt for both ledger and transaction ID`,
		Args: cobra.MaximumNArgs(1),
		RunE: runGet,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Bool("json", false, "Output as JSON")
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
			return fmt.Errorf("invalid transaction ID: %w", err)
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
			return fmt.Errorf("invalid transaction ID: %w", err)
		}
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching transaction #%d...", txID))

	resp, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledgerName,
		TransactionId: txID,
	})
	if err != nil {
		spinner.Fail("Failed to get transaction")
		return cmdutil.FormatGRPCError("failed to get transaction", err)
	}

	_ = spinner.Stop()

	tx := resp.Transaction

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(resp)
	}

	pterm.Println()

	// Display transaction header
	pterm.Printf("Transaction: %s\n", pterm.Cyan(fmt.Sprintf("#%d", tx.Id)))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	// Display basic info
	if tx.Reference != "" {
		pterm.Printf("Reference:   %s\n", tx.Reference)
	}
	if tx.Timestamp != nil {
		pterm.Printf("Timestamp:   %s\n", pterm.Gray(tx.Timestamp.AsTime().Format("2006-01-02T15:04:05Z07:00")))
	}
	if tx.InsertedAt != nil {
		pterm.Printf("Inserted At: %s\n", pterm.Gray(tx.InsertedAt.AsTime().Format("2006-01-02T15:04:05Z07:00")))
	}

	// Display reverted status
	if tx.Reverted {
		pterm.Printf("Reverted:    %s\n", pterm.Yellow("Yes"))
		if tx.RevertedAt != nil {
			pterm.Printf("Reverted At: %s\n", pterm.Gray(tx.RevertedAt.AsTime().Format("2006-01-02T15:04:05Z07:00")))
		}
	} else {
		pterm.Printf("Reverted:    %s\n", pterm.Green("No"))
	}

	// Display receipt if available
	if resp.Receipt != "" {
		pterm.Printf("Receipt:     %s\n", pterm.Gray(resp.Receipt))
	}

	// Display postings
	if len(tx.Postings) > 0 {
		pterm.Println()
		pterm.Println("Postings:")

		postingsTable := pterm.TableData{
			{"#", "SOURCE", "", "DESTINATION", "AMOUNT", "ASSET"},
		}

		termWidth := pterm.GetTerminalWidth()
		// Reserve space for #(3) + arrow(1) + AMOUNT(12) + ASSET(8) + separators(5*3=15) + indent(2)
		const fixedColsWidth = 3 + 1 + 12 + 8 + 15 + 2
		maxAddrWidth := (termWidth - fixedColsWidth) / 2
		if maxAddrWidth < 15 {
			maxAddrWidth = 15
		}

		const continuationIndent = "  "
		for i, posting := range tx.Postings {
			srcLines := cmdutil.WrapText(posting.Source, maxAddrWidth, ":")
			dstLines := cmdutil.WrapText(posting.Destination, maxAddrWidth, ":")

			maxLines := len(srcLines)
			if len(dstLines) > maxLines {
				maxLines = len(dstLines)
			}

			for line := range maxLines {
				src, dst := "", ""
				num, arrow, amount, asset := "", "", "", ""

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
					num = fmt.Sprintf("%d", i+1)
					arrow = "→"
					amount = posting.Amount.Dec()
					asset = posting.Asset
				}

				postingsTable = append(postingsTable, []string{
					num, src, arrow, dst, amount, asset,
				})
			}
		}

		if err := pterm.DefaultTable.WithHasHeader().WithData(postingsTable).Render(); err != nil {
			return err
		}
	}

	// Display metadata
	if tx.Metadata != nil && len(tx.Metadata.Metadata) > 0 {
		pterm.Println()
		pterm.Println("Metadata:")

		metadataTable := pterm.TableData{
			{"KEY", "VALUE"},
		}
		for _, md := range tx.Metadata.Metadata {
			metadataTable = append(metadataTable, []string{
				md.Key,
				commonpb.MetadataValueToString(md.Value),
			})
		}
		return pterm.DefaultTable.WithHasHeader().WithData(metadataTable).Render()
	}

	return nil
}
