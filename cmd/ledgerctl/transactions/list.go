package transactions

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/filterexpr"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewListCommand creates the transactions list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "l"},
		Short:   "List transactions in a ledger",
		Long: `List transactions in a ledger via gRPC with pagination.

Transactions are displayed newest first, with interactive pagination.
Press Enter to load the next page, or 'q' to quit.

If --ledger is not provided and only one ledger exists, it will be used automatically.
If multiple ledgers exist, you will be prompted to select one.

Examples:
  ledgerctl transactions list --ledger my-ledger
  ledgerctl transactions list --ledger my-ledger --page-size 20
  ledgerctl transactions list --ledger my-ledger --filter "metadata[category] == premium"
  ledgerctl transactions list --ledger my-ledger --filter "metadata[status] == pending or metadata[priority] == high"
  ledgerctl transactions list --all   # Fetch all transactions without pagination`,
		RunE: runList,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Uint32("page-size", cmdutil.DefaultPageSize, "Number of transactions per page")
	cmd.Flags().String("filter", "", `Filter expression (e.g. "metadata[category] == premium")`)
	cmd.Flags().Bool("all", false, "Fetch all transactions at once (no pagination)")
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runList(cmd *cobra.Command, _ []string) error {
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

	pageSize, _ := cmd.Flags().GetUint32("page-size")
	filterExpr, _ := cmd.Flags().GetString("filter")
	fetchAll, _ := cmd.Flags().GetBool("all")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	filter, err := buildTransactionFilter(filterExpr)
	if err != nil {
		return err
	}

	if fetchAll {
		return fetchAllTransactions(cmd, client, ledgerName, filter, jsonOutput)
	}

	return fetchTransactionsWithPager(cmd, client, ledgerName, pageSize, filter, jsonOutput)
}

// buildTransactionFilter parses the --filter expression for transaction metadata.
func buildTransactionFilter(filterExpr string) (*commonpb.QueryFilter, error) {
	if filterExpr == "" {
		return nil, nil
	}
	filter, err := filterexpr.Parse(filterExpr)
	if err != nil {
		return nil, fmt.Errorf("invalid filter expression: %w", err)
	}
	return filter, nil
}

func fetchAllTransactions(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, filter *commonpb.QueryFilter, jsonOutput bool) error {
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Fetching all transactions...")

	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger:   ledgerName,
		PageSize: 0, // No limit
		Filter:   filter,
	})
	if err != nil {
		spinner.Fail("Failed to list transactions")
		return cmdutil.FormatGRPCError("failed to list transactions", err)
	}

	var transactions []*commonpb.Transaction
	for {
		tx, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			spinner.Fail("Failed to receive transaction")
			return cmdutil.FormatGRPCError("failed to receive transaction", err)
		}
		transactions = append(transactions, tx)
	}

	_ = spinner.Stop()

	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(transactions)
	}

	if len(transactions) == 0 {
		pterm.Println("No transactions found in this ledger.")
		pterm.Println(pterm.Gray("Create one with: ledgerctl transactions create --ledger " + ledgerName))
		return nil
	}

	renderTransactionsTable(transactions)
	return nil
}

func fetchTransactionsWithPager(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, pageSize uint32, filter *commonpb.QueryFilter, jsonOutput bool) error {
	var afterTxID uint64
	pageNum := 1

	for {
		ctx, cancel := cmdutil.GetContext(cmd)

		spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching page %d...", pageNum))

		stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
			Ledger:    ledgerName,
			PageSize:  pageSize,
			AfterTxId: afterTxID,
			Filter:    filter,
		})
		if err != nil {
			cancel()
			spinner.Fail("Failed to list transactions")
			return cmdutil.FormatGRPCError("failed to list transactions", err)
		}

		var transactions []*commonpb.Transaction
		for {
			tx, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				cancel()
				spinner.Fail("Failed to receive transaction")
				return cmdutil.FormatGRPCError("failed to receive transaction", err)
			}
			transactions = append(transactions, tx)
		}

		cancel()

		if len(transactions) == 0 {
			spinner.Info("No more transactions.")
			if pageNum == 1 {
				pterm.Println("No transactions found in this ledger.")
				pterm.Println(pterm.Gray("Create one with: ledgerctl transactions create --ledger " + ledgerName))
			}
			return nil
		}

		_ = spinner.Stop()

		if jsonOutput {
			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			if err := encoder.Encode(transactions); err != nil {
				return err
			}
		} else {
			pterm.Println()
			pterm.Printf("Transactions (Page %d)\n", pageNum)
			pterm.Println(pterm.Gray("─────────────────────────────────"))
			renderTransactionsTable(transactions)
		}

		// If we got fewer transactions than pageSize, we've reached the end
		if uint32(len(transactions)) < pageSize {
			if !jsonOutput {
				pterm.Info.Println("End of transactions.")
			}
			return nil
		}

		// Update afterTxID for next page (last transaction in current page)
		afterTxID = transactions[len(transactions)-1].Id

		// Prompt for next page
		if !jsonOutput {
			result, err := pterm.DefaultInteractiveConfirm.
				WithDefaultText("Load next page?").
				WithDefaultValue(true).
				Show()
			if err != nil {
				return fmt.Errorf("failed to read input: %w", err)
			}
			if !result {
				return nil
			}
		} else {
			// In JSON mode, don't paginate interactively - just stop
			return nil
		}

		pageNum++
	}
}

func renderTransactionsTable(transactions []*commonpb.Transaction) {
	tableData := pterm.TableData{
		{"ID", "TIMESTAMP", "REFERENCE", "POSTINGS", "STATUS"},
	}

	for _, tx := range transactions {
		timestamp := "-"
		if tx.Timestamp != nil {
			timestamp = tx.Timestamp.AsTime().Format(time.RFC3339)
		}

		reference := "-"
		if tx.Reference != "" {
			reference = tx.Reference
		}

		status := pterm.Green("OK")
		if tx.Reverted {
			status = pterm.Yellow("Reverted")
		}

		tableData = append(tableData, []string{
			fmt.Sprintf("%d", tx.Id),
			timestamp,
			reference,
			fmt.Sprintf("%d", len(tx.Postings)),
			status,
		})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}
