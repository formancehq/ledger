package accounts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// NewListCommand creates the accounts list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: cmdutil.ListAliases,
		Short:   "List accounts in a ledger",
		Long: `List accounts in a ledger via gRPC with pagination.

Accounts are displayed in alphabetical order by default. Use --reverse for Z→A.
Press Enter to load the next page, or 'q' to quit.

If --ledger is not provided and only one ledger exists, it will be used automatically.
If multiple ledgers exist, you will be prompted to select one.

Examples:
  ledgerctl accounts list --ledger my-ledger
  ledgerctl accounts list --ledger my-ledger --page-size 20
  ledgerctl accounts list --ledger my-ledger --prefix users:
  ledgerctl accounts list --ledger my-ledger --filter "metadata[category] == premium"
  ledgerctl accounts list --ledger my-ledger --filter "metadata[active] == true or address ^= users:"
  ledgerctl accounts list --reverse   # Reverse alphabetical (Z→A)
  ledgerctl accounts list --all   # Fetch all accounts without pagination
  ledgerctl accounts list --cursor eyJrZXkiOiJ1c2Vyczpib2IifQ   # Resume after users:bob (page token for {"key":"users:bob"})`,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{
		SupportsReverse: true,
		SupportsAll:     true,
	})
	cmdutil.AddFilterFlags(cmd, cmdutil.FilterOptions{SupportsPrefix: true})
	cmdutil.AddConsistencyFlags(cmd)
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmdutil.AddAnalyzeFlag(cmd)

	return cmd
}

// ListPageFetcher fetches one page through the host's chosen executor. It must
// preserve the supplied context, ordering, page size, and cursor, and return
// trailers so profiling and adjacent-page hints stay available to the host.
type ListPageFetcher func(context.Context, cmdutil.PaginationFlags) ([]*commonpb.Account, metadata.MD, error)

// RunListWithFetch retains ledgerctl's --all, interactive pager, structured
// output, and --analyze behavior while leaving transport to the callback.
// Received items are rendered before returning a fetch failure; no page is retried.
func RunListWithFetch(cmd *cobra.Command, fetch ListPageFetcher) error {
	pgn := cmdutil.GetPaginationFlags(cmd)
	showProfile, _ := cmd.Flags().GetBool("analyze")
	if pgn.All {
		return fetchAllAccounts(cmd, fetch, pgn.Cursor, pgn.Reverse, showProfile)
	}

	return fetchAccountsWithPager(cmd, fetch, pgn, showProfile)
}

func fetchAllAccounts(cmd *cobra.Command, fetch ListPageFetcher, initialCursor string, reverse bool, showProfile bool) error {
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	if showProfile {
		ctx = cmdutil.ProfileContext(ctx)
	}

	spinner := cmdutil.StartSpinner("Fetching all accounts...")

	var lastTrailer metadata.MD

	accounts, err := cmdutil.DrainAllPages(initialCursor, func(cur string) ([]*commonpb.Account, metadata.MD, error) {
		items, trailer, err := fetch(ctx, cmdutil.PaginationFlags{Cursor: cur, Reverse: reverse})
		lastTrailer = trailer

		return items, trailer, err
	})

	_ = spinner.Stop()

	if err != nil {
		if len(accounts) == 0 {
			return err
		}
		renderErr := renderListPage(cmd, accounts, 0)
		if renderErr == nil && showProfile && lastTrailer != nil {
			renderListProfile(cmd, lastTrailer)
		}
		cmdutil.EmitCursorHints(cmd, cmdutil.CursorsFromTrailer(lastTrailer))

		return errors.Join(err, renderErr)
	}

	if err := renderListPage(cmd, accounts, 0); err != nil {
		return err
	}

	if showProfile && lastTrailer != nil {
		renderListProfile(cmd, lastTrailer)
	}

	return nil
}

func fetchAccountsWithPager(cmd *cobra.Command, fetch ListPageFetcher, pgn cmdutil.PaginationFlags, showProfile bool) error {
	page := pgn
	pageNum := 1

	for {
		ctx, cancel := cmdutil.GetContext(cmd)
		if showProfile {
			ctx = cmdutil.ProfileContext(ctx)
		}

		spinner := cmdutil.StartSpinner(fmt.Sprintf("Fetching page %d...", pageNum))

		accounts, trailer, err := fetch(ctx, page)
		cancel()
		if err != nil {
			_ = spinner.Stop()
			if len(accounts) == 0 {
				return err
			}
			renderErr := renderListPage(cmd, accounts, pageNum)
			if renderErr == nil && showProfile {
				renderListProfile(cmd, trailer)
			}
			cmdutil.EmitCursorHints(cmd, cmdutil.CursorsFromTrailer(trailer))

			return errors.Join(err, renderErr)
		}

		if len(accounts) == 0 {
			if cmdutil.IsStructuredOutput(cmd) {
				_ = spinner.Stop()
				if err := renderListPage(cmd, accounts, pageNum); err != nil {
					return err
				}
			} else {
				spinner.Info("No more accounts.")
				if pageNum == 1 {
					pterm.Info.WithWriter(cmd.OutOrStdout()).Println("No accounts found.")
					pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout()).Println(pterm.Gray("Create transactions to populate accounts."))
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

		if err := renderListPage(cmd, accounts, pageNum); err != nil {
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
			pterm.Info.WithWriter(cmd.OutOrStdout()).Println("End of accounts.")
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
func RenderList(cmd *cobra.Command, accounts []*commonpb.Account, cursors cmdutil.PageCursors) error {
	pageNum := 1
	if cmdutil.GetPaginationFlags(cmd).All {
		pageNum = 0
	}
	if err := renderListPage(cmd, accounts, pageNum); err != nil {
		return err
	}
	cmdutil.EmitCursorHints(cmd, cursors)

	return nil
}

func renderListPage(cmd *cobra.Command, accounts []*commonpb.Account, pageNum int) error {
	if handled, err := cmdutil.EncodeStructured(cmd, accounts); handled || err != nil {
		return err
	}
	if len(accounts) == 0 {
		pterm.Info.WithWriter(cmd.OutOrStdout()).Println("No accounts found.")
		pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout()).Println(pterm.Gray("Create transactions to populate accounts."))

		return nil
	}
	if pageNum > 0 {
		pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout()).Println()
		pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout()).Printf("Accounts (Page %d)\n", pageNum)
		pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout()).Println(pterm.Gray("─────────────────────────────────"))
	}

	return renderAccountsTable(accounts, cmdutil.RescaleTarget(cmd), cmd.OutOrStdout())
}

func renderAccountsTable(accounts []*commonpb.Account, rescale *uint8, writer io.Writer) error {
	termWidth := pterm.GetTerminalWidth()

	const (
		metadataColWidth   = 8
		balanceColWidth    = 28
		separatorWidth     = 3
		continuationIndent = "  "
	)

	// Two extra separators now (ADDRESS | BALANCES | METADATA).
	maxAddressWidth := max(termWidth-balanceColWidth-metadataColWidth-2*separatorWidth-len(continuationIndent), 20)

	tableData := pterm.TableData{
		{"ADDRESS", "BALANCES", "METADATA"},
	}

	for _, account := range accounts {
		metadataCount := strconv.Itoa(len(account.GetMetadata()))

		addressLines := cmdutil.WrapText(account.GetAddress(), maxAddressWidth, ":")
		balanceLines, err := formatAccountBalances(account.GetVolumes(), rescale)
		if err != nil {
			return err
		}

		// An account row spans as many lines as its longest column so wrapped
		// addresses and multi-asset balances stay vertically aligned.
		rowCount := max(len(addressLines), len(balanceLines))
		for i := range rowCount {
			var address, balance, metadata string

			if i < len(addressLines) {
				if i == 0 {
					address = addressLines[0]
				} else {
					address = continuationIndent + addressLines[i]
				}
			}

			if i < len(balanceLines) {
				balance = balanceLines[i]
			}

			if i == 0 {
				metadata = metadataCount
			}

			tableData = append(tableData, []string{address, balance, metadata})
		}
	}

	return pterm.DefaultTable.WithWriter(writer).WithHasHeader().WithData(tableData).Render()
}

// formatAccountBalances renders one "ASSET balance" line per (asset, color)
// bucket, coloring negative balances red and the rest green (matching the
// accounts get view). Colored buckets carry a "[COLOR]" marker; the uncolored
// bucket is labelled by its asset alone. Returns a single muted placeholder when
// there are no volumes, and an error when --rescale meets a volume that is not a
// canonical integer (see cmdutil.AggregateVolumes).
func formatAccountBalances(volumes []*commonpb.AccountVolume, rescale *uint8) ([]string, error) {
	if len(volumes) == 0 {
		return []string{pterm.Gray("—")}, nil
	}

	// With --rescale, currencies that differ only in precision (USD/4, USD/8)
	// collapse to a single base currency per color, their balances are summed,
	// and the sum is re-expressed at the requested scale.
	if rescale != nil {
		raw := make([]cmdutil.RawVolume, 0, len(volumes))
		for _, entry := range volumes {
			vol := entry.GetVolumes()
			if vol == nil {
				return nil, fmt.Errorf("formatAccountBalances: account %s asset %q color %q has no Volumes container",
					"(unknown)", entry.GetAsset(), entry.GetColor())
			}
			if err := vol.Validate(); err != nil {
				return nil, fmt.Errorf("formatAccountBalances: account %s asset %q color %q is malformed: %w",
					"(unknown)", entry.GetAsset(), entry.GetColor(), err)
			}
			inputStr, err := vol.GetInput().Dec()
			if err != nil {
				return nil, fmt.Errorf("formatAccountBalances: account %s asset %q color %q input is malformed: %w",
					"(unknown)", entry.GetAsset(), entry.GetColor(), err)
			}
			outputStr, err := vol.GetOutput().Dec()
			if err != nil {
				return nil, fmt.Errorf("formatAccountBalances: account %s asset %q color %q output is malformed: %w",
					"(unknown)", entry.GetAsset(), entry.GetColor(), err)
			}
			raw = append(raw, cmdutil.RawVolume{
				Asset:  entry.GetAsset(),
				Color:  entry.GetColor(),
				Input:  inputStr,
				Output: outputStr,
			})
		}

		aggregated, err := cmdutil.AggregateVolumes(raw)
		if err != nil {
			return nil, err
		}

		lines := make([]string, 0, len(aggregated))
		for _, av := range aggregated {
			balanceColor := pterm.Green
			if av.Balance.Sign() < 0 {
				balanceColor = pterm.Red
			}

			balance := cmdutil.RescaleAmount(av.Balance, av.Precision, *rescale)
			label := balanceLabel(invariants.FormatAsset(av.Asset, *rescale), av.Color)
			lines = append(lines, fmt.Sprintf("%s %s", label, balanceColor(balance)))
		}

		return lines, nil
	}

	// volumes is already sorted by (asset, color) ascending server-side, so we
	// just render in-order.
	lines := make([]string, 0, len(volumes))

	for _, entry := range volumes {
		vol := entry.GetVolumes()
		if vol == nil {
			return nil, fmt.Errorf("formatAccountBalances: account %s asset %q color %q has no Volumes container",
				"(unknown)", entry.GetAsset(), entry.GetColor())
		}
		if err := vol.Validate(); err != nil {
			return nil, fmt.Errorf("formatAccountBalances: account %s asset %q color %q is malformed: %w",
				"(unknown)", entry.GetAsset(), entry.GetColor(), err)
		}
		balance, err := vol.GetBalance().Dec()
		if err != nil {
			return nil, fmt.Errorf("formatAccountBalances: account %s asset %q color %q balance is malformed: %w",
				"(unknown)", entry.GetAsset(), entry.GetColor(), err)
		}

		balanceColor := pterm.Green
		if len(balance) > 0 && balance[0] == '-' {
			balanceColor = pterm.Red
		}

		label := balanceLabel(entry.GetAsset(), entry.GetColor())
		lines = append(lines, fmt.Sprintf("%s %s", label, balanceColor(balance)))
	}

	return lines, nil
}

// balanceLabel names a balance bucket: the asset alone for the uncolored bucket,
// or the asset followed by a "[COLOR]" marker for a colored one. The BALANCES
// column is a single column, so the color cannot get a header of its own.
func balanceLabel(asset, color string) string {
	if color == "" {
		return asset
	}

	return fmt.Sprintf("%s[%s]", asset, color)
}
