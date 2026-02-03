package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

const defaultPageSize = 10

// newTransactionsListCommand creates the transactions list command.
func newTransactionsListCommand() *cobra.Command {
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
  ledgerctl transactions list --all   # Fetch all transactions without pagination`,
		RunE: runTransactionsList,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Uint32("page-size", defaultPageSize, "Number of transactions per page")
	cmd.Flags().Bool("all", false, "Fetch all transactions at once (no pagination)")
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runTransactionsList(cmd *cobra.Command, _ []string) error {
	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	// Get ledger name (from flag or interactive selection)
	ledgerFlag, _ := cmd.Flags().GetString("ledger")
	ledgerName, err := selectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

	pageSize, _ := cmd.Flags().GetUint32("page-size")
	fetchAll, _ := cmd.Flags().GetBool("all")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	if fetchAll {
		return fetchAllTransactions(cmd, client, ledgerName, jsonOutput)
	}

	return fetchTransactionsWithPager(cmd, client, ledgerName, pageSize, jsonOutput)
}

func fetchAllTransactions(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, jsonOutput bool) error {
	ctx, cancel := getContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Fetching all transactions...")

	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger:   ledgerName,
		PageSize: 0, // No limit
	})
	if err != nil {
		spinner.Fail("Failed to list transactions")
		return fmt.Errorf("failed to list transactions: %w", err)
	}

	var transactions []*commonpb.Transaction
	for {
		tx, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			spinner.Fail("Failed to receive transaction")
			return fmt.Errorf("failed to receive transaction: %w", err)
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

func fetchTransactionsWithPager(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, pageSize uint32, jsonOutput bool) error {
	var afterTxID uint64
	pageNum := 1

	for {
		ctx, cancel := getContext(cmd)

		spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching page %d...", pageNum))

		stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
			Ledger:    ledgerName,
			PageSize:  pageSize,
			AfterTxId: afterTxID,
		})
		if err != nil {
			cancel()
			spinner.Fail("Failed to list transactions")
			return fmt.Errorf("failed to list transactions: %w", err)
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
				return fmt.Errorf("failed to receive transaction: %w", err)
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
