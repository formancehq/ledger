package transactions

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewListCommand creates the transactions list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: cmdutil.ListAliases,
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
  ledgerctl transactions list --ledger my-ledger --filter "metadata[block_height] between 800000 and 800099"
  ledgerctl transactions list --ledger my-ledger --filter 'source ^= "merchants:"'
  ledgerctl transactions list --ledger my-ledger --filter 'destination == "users:alice"'
  ledgerctl transactions list --ledger my-ledger --filter 'source ^= "merchants:" and destination ^= "users:"'
  ledgerctl transactions list --reverse   # Oldest first
  ledgerctl transactions list --all   # Fetch all transactions without pagination
  ledgerctl transactions list --cursor 42   # Resume after tx id 42`,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runList,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{
		SupportsReverse: true,
		SupportsAll:     true,
	})
	cmdutil.AddFilterFlags(cmd, cmdutil.FilterOptions{})
	cmdutil.AddConsistencyFlags(cmd)
	cmdutil.AddOutputFlags(cmd)
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

	ledgerFlag, _ := cmd.Flags().GetString("ledger")

	ledgerName, err := cmdutil.SelectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

	pgn := cmdutil.GetPaginationFlags(cmd)
	flt := cmdutil.GetFilterFlags(cmd)
	cns := cmdutil.GetConsistencyFlags(cmd)
	showProfile, _ := cmd.Flags().GetBool("analyze")

	filter, err := cmdutil.BuildQueryFilter(flt.Expr, flt.Prefix)
	if err != nil {
		return err
	}

	if pgn.All {
		return fetchAllTransactions(cmd, client, ledgerName, filter, pgn.Cursor, pgn.Reverse, cns, showProfile)
	}

	return fetchTransactionsWithPager(cmd, client, ledgerName, pgn, filter, cns, showProfile)
}

func fetchAllTransactions(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, filter *commonpb.QueryFilter, initialCursor string, reverse bool, cns cmdutil.ConsistencyFlags, showProfile bool) error {
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	if showProfile {
		ctx = cmdutil.ProfileContext(ctx)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching all transactions...")

	var lastTrailer metadata.MD

	transactions, err := cmdutil.DrainAllPages(initialCursor, func(cur string) ([]*commonpb.Transaction, metadata.MD, error) {
		stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
			Ledger:  ledgerName,
			Options: cmdutil.BuildListOptions(cmdutil.PaginationFlags{Cursor: cur, Reverse: reverse}, cns, filter),
		})
		if err != nil {
			return nil, nil, cmdutil.FormatGRPCError("failed to list transactions", err)
		}

		items, recvErr := cmdutil.CollectStream(stream)
		if recvErr != nil {
			return nil, nil, cmdutil.FormatGRPCError("failed to receive transaction", recvErr)
		}

		lastTrailer = stream.Trailer()

		return items, lastTrailer, nil
	})

	_ = spinner.Stop()

	if err != nil {
		return err
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

	if showProfile && lastTrailer != nil {
		cmdutil.RenderProfile(cmdutil.ExtractProfile(lastTrailer))
	}

	return nil
}

func fetchTransactionsWithPager(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, pgn cmdutil.PaginationFlags, filter *commonpb.QueryFilter, cns cmdutil.ConsistencyFlags, showProfile bool) error {
	page := pgn
	pageNum := 1

	for {
		ctx, cancel := cmdutil.GetContext(cmd)
		if showProfile {
			ctx = cmdutil.ProfileContext(ctx)
		}

		spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching page %d...", pageNum))

		stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
			Ledger:  ledgerName,
			Options: cmdutil.BuildListOptions(page, cns, filter),
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

		nextCursor := cmdutil.NextCursorFromTrailer(stream.Trailer())
		if nextCursor == "" {
			if !structuredOutput {
				pterm.Info.Println("End of transactions.")
			}

			return nil
		}

		page.Cursor = nextCursor

		if structuredOutput {
			// `transactions list --json/--yaml` printed the JSON/YAML payload
			// on stdout above; surface the resume cursor on stderr so scripts
			// can pick it up without parsing gRPC trailers.
			cmdutil.EmitNextCursorHint(cmd, nextCursor)

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
