package transactions

import (
	"context"
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
  ledgerctl transactions list --cursor eyJrZXkiOiI0MiJ9   # Resume after tx id 42 (page token for {"key":"42"})`,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
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

// ListPageFetcher fetches one page through the host's chosen executor. It must
// preserve the supplied context, ordering, page size, and cursor, and return
// trailers so profiling and adjacent-page hints stay available to the host.
type ListPageFetcher func(context.Context, cmdutil.PaginationFlags) ([]*commonpb.Transaction, metadata.MD, error)

// RunListWithFetch retains ledgerctl's --all, interactive pager, structured
// output, and --analyze behavior while leaving transport to the callback.
// Received items are rendered before returning a fetch failure; no page is retried.
func RunListWithFetch(cmd *cobra.Command, ledgerName string, fetch ListPageFetcher) error {
	pgn := cmdutil.GetPaginationFlags(cmd)
	showProfile, _ := cmd.Flags().GetBool("analyze")
	if pgn.All {
		return fetchAllTransactions(cmd, ledgerName, fetch, pgn.Cursor, pgn.Reverse, showProfile)
	}

	return fetchTransactionsWithPager(cmd, ledgerName, fetch, pgn, showProfile)
}

func fetchAllTransactions(cmd *cobra.Command, ledgerName string, fetch ListPageFetcher, initialCursor string, reverse bool, showProfile bool) error {
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	if showProfile {
		ctx = cmdutil.ProfileContext(ctx)
	}

	spinner := cmdutil.StartSpinner("Fetching all transactions...")

	var lastTrailer metadata.MD

	transactions, err := cmdutil.DrainAllPages(initialCursor, func(cur string) ([]*commonpb.Transaction, metadata.MD, error) {
		items, trailer, err := fetch(ctx, cmdutil.PaginationFlags{Cursor: cur, Reverse: reverse})
		lastTrailer = trailer

		return items, trailer, err
	})

	_ = spinner.Stop()

	if err != nil {
		if len(transactions) == 0 {
			return err
		}
		renderErr := renderListPage(cmd, transactions, 0, ledgerName)
		if renderErr == nil && showProfile && lastTrailer != nil {
			renderListProfile(cmd, lastTrailer)
		}
		cmdutil.EmitCursorHints(cmd, cmdutil.CursorsFromTrailer(lastTrailer))

		return errors.Join(err, renderErr)
	}

	if err := renderListPage(cmd, transactions, 0, ledgerName); err != nil {
		return err
	}

	if showProfile && lastTrailer != nil {
		renderListProfile(cmd, lastTrailer)
	}

	return nil
}

func fetchTransactionsWithPager(cmd *cobra.Command, ledgerName string, fetch ListPageFetcher, pgn cmdutil.PaginationFlags, showProfile bool) error {
	page := pgn
	pageNum := 1

	for {
		ctx, cancel := cmdutil.GetContext(cmd)
		if showProfile {
			ctx = cmdutil.ProfileContext(ctx)
		}

		spinner := cmdutil.StartSpinner(fmt.Sprintf("Fetching page %d...", pageNum))

		transactions, trailer, err := fetch(ctx, page)
		cancel()
		if err != nil {
			_ = spinner.Stop()
			if len(transactions) == 0 {
				return err
			}
			renderErr := renderListPage(cmd, transactions, pageNum, ledgerName)
			if renderErr == nil && showProfile {
				renderListProfile(cmd, trailer)
			}
			cmdutil.EmitCursorHints(cmd, cmdutil.CursorsFromTrailer(trailer))

			return errors.Join(err, renderErr)
		}

		if len(transactions) == 0 {
			if cmdutil.IsStructuredOutput(cmd) {
				_ = spinner.Stop()
				if err := renderListPage(cmd, transactions, pageNum, ledgerName); err != nil {
					return err
				}
			} else {
				spinner.Info("No more transactions.")
				if pageNum == 1 {
					pterm.Info.WithWriter(cmd.OutOrStdout()).Println("No transactions found.")
					pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout()).Println(pterm.Gray("Create one with: ledgerctl transactions create --ledger " + ledgerName))
				}
			}
			if showProfile {
				renderListProfile(cmd, trailer)
			}
			cmdutil.EmitCursorHints(cmd, cmdutil.CursorsFromTrailer(trailer))

			return nil
		}

		_ = spinner.Stop()

		structuredOutput := cmdutil.IsStructuredOutput(cmd)

		if err := renderListPage(cmd, transactions, pageNum, ledgerName); err != nil {
			return err
		}

		if showProfile {
			renderListProfile(cmd, trailer)
		}

		cursors := cmdutil.CursorsFromTrailer(trailer)

		if structuredOutput {
			// The JSON/YAML payload went to stdout above; the page tokens go to
			// stderr so scripts can pick them up without parsing gRPC trailers.
			cmdutil.EmitCursorHints(cmd, cursors)

			return nil
		}

		if cursors.Next == "" {
			pterm.Info.WithWriter(cmd.OutOrStdout()).Println("End of transactions.")
			cmdutil.EmitPreviousCursorHint(cmd, cursors.Previous)

			return nil
		}

		page.Cursor = cursors.Next

		// Declining the prompt below ends the walk, so both tokens are shown
		// first.
		cmdutil.EmitCursorHints(cmd, cursors)

		result, err := cmdutil.ConfirmNextPage(cmd)
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		if !result {
			return nil
		}

		pageNum++
	}
}

func renderListProfile(cmd *cobra.Command, trailer metadata.MD) {
	output := cmd.OutOrStdout()
	if cmdutil.IsStructuredOutput(cmd) {
		output = cmd.ErrOrStderr()
	}
	cmdutil.RenderProfileTo(output, cmdutil.ExtractProfile(trailer))
}

// RenderList writes one already fetched page. Cursor hints stay separate from
// structured output; fetching additional pages belongs to RunListWithFetch.
func RenderList(cmd *cobra.Command, transactions []*commonpb.Transaction, cursors cmdutil.PageCursors) error {
	pageNum := 1
	if cmdutil.GetPaginationFlags(cmd).All {
		pageNum = 0
	}
	ledgerName, _ := cmd.Flags().GetString("ledger")
	if err := renderListPage(cmd, transactions, pageNum, ledgerName); err != nil {
		return err
	}
	cmdutil.EmitCursorHints(cmd, cursors)

	return nil
}

func renderListPage(cmd *cobra.Command, transactions []*commonpb.Transaction, pageNum int, ledgerName string) error {
	if handled, err := cmdutil.EncodeStructured(cmd, transactions); handled || err != nil {
		return err
	}
	if len(transactions) == 0 {
		pterm.Info.WithWriter(cmd.OutOrStdout()).Println("No transactions found.")
		pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout()).Println(pterm.Gray("Create one with: ledgerctl transactions create --ledger " + ledgerName))

		return nil
	}
	if pageNum > 0 {
		pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout()).Println()
		pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout()).Printf("Transactions (Page %d)\n", pageNum)
		pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout()).Println(pterm.Gray("─────────────────────────────────"))
	}

	return renderTransactionsTable(transactions, cmd.OutOrStdout())
}

func renderTransactionsTable(transactions []*commonpb.Transaction, writer io.Writer) error {
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

	return pterm.DefaultTable.WithWriter(writer).WithHasHeader().WithData(tableData).Render()
}
