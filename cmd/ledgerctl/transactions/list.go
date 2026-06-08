package transactions

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewListCommand creates the transactions list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "l"},
		Short:   "List transactions in a ledger",
		Long: `List transactions in a ledger via gRPC with pagination.

Transactions are displayed newest first by default. Use --reverse for oldest first.
Press Enter to load the next page, or 'q' to quit.

If --ledger is not provided and only one ledger exists, it will be used automatically.
If multiple ledgers exist, you will be prompted to select one.

Examples:
  ledgerctl transactions list --ledger my-ledger
  ledgerctl transactions list --ledger my-ledger --page-size 20
  ledgerctl transactions list --ledger my-ledger --filter "metadata[category] == premium"
  ledgerctl transactions list --ledger my-ledger --filter "metadata[status] == pending or metadata[priority] == high"
  ledgerctl transactions list --ledger my-ledger --filter 'source ^= "merchants:"'
  ledgerctl transactions list --ledger my-ledger --filter 'destination == "users:alice"'
  ledgerctl transactions list --ledger my-ledger --filter 'source ^= "merchants:" and destination ^= "users:"'
  ledgerctl transactions list --reverse   # Oldest first
  ledgerctl transactions list --all   # Fetch all transactions without pagination`,
		RunE: runList,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Uint32("page-size", cmdutil.DefaultPageSize, "Number of transactions per page")
	cmd.Flags().String("filter", "", `Filter expression (e.g. "metadata[category] == premium")`)
	cmd.Flags().Bool("reverse", false, "Reverse iteration order (oldest first instead of newest first)")
	cmd.Flags().Bool("all", false, "Fetch all transactions at once (no pagination)")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Uint64("min-log-sequence", 0, "Minimum log sequence the server must have applied before reading (0 = no constraint)")
	cmd.Flags().Uint64("checkpoint-id", 0, "Read from a query checkpoint instead of the live store")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmdutil.AddAnalyzeFlag(cmd)

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
	reverse, _ := cmd.Flags().GetBool("reverse")
	fetchAll, _ := cmd.Flags().GetBool("all")
	minLogSeq, _ := cmd.Flags().GetUint64("min-log-sequence")
	checkpointID, _ := cmd.Flags().GetUint64("checkpoint-id")
	showProfile, _ := cmd.Flags().GetBool("analyze")

	filter, err := buildTransactionFilter(filterExpr)
	if err != nil {
		return err
	}

	if fetchAll {
		return fetchAllTransactions(cmd, client, ledgerName, filter, reverse, minLogSeq, checkpointID, showProfile)
	}

	return fetchTransactionsWithPager(cmd, client, ledgerName, pageSize, filter, reverse, minLogSeq, checkpointID, showProfile)
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

func fetchAllTransactions(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, filter *commonpb.QueryFilter, reverse bool, minLogSeq, checkpointID uint64, showProfile bool) error {
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	if showProfile {
		ctx = cmdutil.ProfileContext(ctx)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching all transactions...")

	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger:         ledgerName,
		PageSize:       0, // No limit
		Filter:         filter,
		Reverse:        reverse,
		MinLogSequence: minLogSeq,
		CheckpointId:   checkpointID,
	})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to list transactions", err)
	}

	transactions, err := cmdutil.CollectStream(stream)

	_ = spinner.Stop()

	if err != nil {
		return cmdutil.FormatGRPCError("failed to receive transaction", err)
	}

	handled, err := cmdutil.EncodeStructured(cmd, transactions)
	if err != nil {
		return err
	}

	switch {
	case handled:
		// Structured output already written.
	case len(transactions) == 0:
		pterm.Info.Println("No transactions found.")
		pterm.Println(pterm.Gray("Create one with: ledgerctl transactions create --ledger " + ledgerName))
	default:
		renderTransactionsTable(transactions)
	}

	if showProfile {
		cmdutil.RenderProfile(cmdutil.ExtractProfile(stream.Trailer()))
	}

	return nil
}

func fetchTransactionsWithPager(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, pageSize uint32, filter *commonpb.QueryFilter, reverse bool, minLogSeq, checkpointID uint64, showProfile bool) error {
	var afterTxID uint64

	pageNum := 1

	for {
		ctx, cancel := cmdutil.GetContext(cmd)
		if showProfile {
			ctx = cmdutil.ProfileContext(ctx)
		}

		spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching page %d...", pageNum))

		stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
			Ledger:         ledgerName,
			PageSize:       pageSize,
			AfterTxId:      afterTxID,
			Filter:         filter,
			Reverse:        reverse,
			MinLogSequence: minLogSeq,
			CheckpointId:   checkpointID,
		})
		if err != nil {
			cancel()

			_ = spinner.Stop()

			return cmdutil.FormatGRPCError("failed to list transactions", err)
		}

		var transactions []*commonpb.Transaction

		for {
			tx, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				cancel()

				_ = spinner.Stop()

				return cmdutil.FormatGRPCError("failed to receive transaction", err)
			}

			transactions = append(transactions, tx)
		}

		cancel()

		if len(transactions) == 0 {
			spinner.Info("No more transactions.")

			if pageNum == 1 {
				pterm.Info.Println("No transactions found.")
				pterm.Println(pterm.Gray("Create one with: ledgerctl transactions create --ledger " + ledgerName))
			}

			if showProfile {
				cmdutil.RenderProfile(cmdutil.ExtractProfile(stream.Trailer()))
			}

			return nil
		}

		_ = spinner.Stop()

		structuredOutput := cmdutil.IsStructuredOutput(cmd)

		if structuredOutput {
			if handled, err := cmdutil.EncodeStructured(cmd, transactions); handled && err != nil {
				return err
			}
		} else {
			pterm.Println()
			pterm.Printf("Transactions (Page %d)\n", pageNum)
			pterm.Println(pterm.Gray("─────────────────────────────────"))
			renderTransactionsTable(transactions)
		}

		if showProfile {
			cmdutil.RenderProfile(cmdutil.ExtractProfile(stream.Trailer()))
		}

		// If we got fewer transactions than pageSize, we've reached the end
		if uint32(len(transactions)) < pageSize {
			if !structuredOutput {
				pterm.Info.Println("End of transactions.")
			}

			return nil
		}

		// Update afterTxID for next page (last transaction in current page)
		afterTxID = transactions[len(transactions)-1].GetId()

		if structuredOutput {
			return nil
		}

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

		pageNum++
	}
}

func renderTransactionsTable(transactions []*commonpb.Transaction) {
	tableData := pterm.TableData{
		{"ID", "TIMESTAMP", "REFERENCE", "POSTINGS", "STATUS"},
	}

	for _, tx := range transactions {
		timestamp := "-"
		if tx.GetTimestamp() != nil {
			timestamp = tx.GetTimestamp().AsTime().Format(time.RFC3339)
		}

		reference := "-"
		if tx.GetReference() != "" {
			reference = tx.GetReference()
		}

		status := pterm.Green("OK")
		if tx.GetReverted() {
			status = pterm.Yellow("Reverted")
		}

		tableData = append(tableData, []string{
			strconv.FormatUint(tx.GetId(), 10),
			timestamp,
			reference,
			strconv.Itoa(len(tx.GetPostings())),
			status,
		})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}
