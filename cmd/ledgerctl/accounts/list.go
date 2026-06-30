package accounts

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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
  ledgerctl accounts list --cursor users:bob   # Resume after a previous page`,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runList,
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

	filter, err := cmdutil.BuildQueryFilter(flt.Expr, flt.Prefix, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	if err != nil {
		return err
	}

	if pgn.All {
		return fetchAllAccounts(cmd, client, ledgerName, filter, pgn.Cursor, pgn.Reverse, cns, showProfile)
	}

	return fetchAccountsWithPager(cmd, client, ledgerName, pgn, filter, cns, showProfile)
}

func fetchAllAccounts(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, filter *commonpb.QueryFilter, initialCursor string, reverse bool, cns cmdutil.ConsistencyFlags, showProfile bool) error {
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	if showProfile {
		ctx = cmdutil.ProfileContext(ctx)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching all accounts...")

	var lastTrailer metadata.MD

	accounts, err := cmdutil.DrainAllPages(initialCursor, func(cur string) ([]*commonpb.Account, metadata.MD, error) {
		stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
			Ledger:  ledgerName,
			Options: cmdutil.BuildListOptions(cmdutil.PaginationFlags{Cursor: cur, Reverse: reverse}, cns, filter),
		})
		if err != nil {
			return nil, nil, cmdutil.FormatGRPCError("failed to list accounts", err)
		}

		items, recvErr := cmdutil.CollectStream(stream)
		if recvErr != nil {
			return nil, nil, cmdutil.FormatGRPCError("failed to receive account", recvErr)
		}

		lastTrailer = stream.Trailer()

		return items, lastTrailer, nil
	})

	_ = spinner.Stop()

	if err != nil {
		return err
	}

	handled, err := cmdutil.EncodeStructured(cmd, accounts)
	if err != nil {
		return err
	}

	switch {
	case handled:
		// Structured output already written.
	case len(accounts) == 0:
		pterm.Info.Println("No accounts found.")
		pterm.Println(pterm.Gray("Create transactions to populate accounts."))
	default:
		renderAccountsTable(accounts)
	}

	if showProfile && lastTrailer != nil {
		cmdutil.RenderProfile(cmdutil.ExtractProfile(lastTrailer))
	}

	return nil
}

func fetchAccountsWithPager(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, pgn cmdutil.PaginationFlags, filter *commonpb.QueryFilter, cns cmdutil.ConsistencyFlags, showProfile bool) error {
	page := pgn
	pageNum := 1

	for {
		ctx, cancel := cmdutil.GetContext(cmd)
		if showProfile {
			ctx = cmdutil.ProfileContext(ctx)
		}

		spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching page %d...", pageNum))

		stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
			Ledger:  ledgerName,
			Options: cmdutil.BuildListOptions(page, cns, filter),
		})
		if err != nil {
			cancel()

			_ = spinner.Stop()

			return cmdutil.FormatGRPCError("failed to list accounts", err)
		}

		var accounts []*commonpb.Account

		for {
			account, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				cancel()

				_ = spinner.Stop()

				return cmdutil.FormatGRPCError("failed to receive account", err)
			}

			accounts = append(accounts, account)
		}

		cancel()

		if len(accounts) == 0 {
			spinner.Info("No more accounts.")

			if pageNum == 1 {
				pterm.Info.Println("No accounts found.")
				pterm.Println(pterm.Gray("Create transactions to populate accounts."))
			}

			if showProfile {
				cmdutil.RenderProfile(cmdutil.ExtractProfile(stream.Trailer()))
			}

			return nil
		}

		_ = spinner.Stop()

		structuredOutput := cmdutil.IsStructuredOutput(cmd)

		if structuredOutput {
			if handled, err := cmdutil.EncodeStructured(cmd, accounts); handled && err != nil {
				return err
			}
		} else {
			pterm.Println()
			pterm.Printf("Accounts (Page %d)\n", pageNum)
			pterm.Println(pterm.Gray("─────────────────────────────────"))
			renderAccountsTable(accounts)
		}

		if showProfile {
			cmdutil.RenderProfile(cmdutil.ExtractProfile(stream.Trailer()))
		}

		nextCursor := cmdutil.NextCursorFromTrailer(stream.Trailer())
		if nextCursor == "" {
			if !structuredOutput {
				pterm.Info.Println("End of accounts.")
			}

			return nil
		}

		page.Cursor = nextCursor

		if structuredOutput {
			// `accounts list --json/--yaml` printed the JSON/YAML payload on
			// stdout above; surface the resume cursor on stderr so scripts can
			// pick it up without parsing gRPC trailers.
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

func renderAccountsTable(accounts []*commonpb.Account) {
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
		balanceLines := formatAccountBalances(account.GetVolumes())

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

	_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}

// formatAccountBalances renders one "ASSET balance" line per asset, sorted by
// asset, coloring negative balances red and the rest green (matching the
// accounts get view). Returns a single muted placeholder when there are no
// volumes.
func formatAccountBalances(volumes map[string]*commonpb.VolumesWithBalance) []string {
	if len(volumes) == 0 {
		return []string{pterm.Gray("—")}
	}

	assets := make([]string, 0, len(volumes))
	for asset := range volumes {
		assets = append(assets, asset)
	}

	sort.Strings(assets)

	lines := make([]string, 0, len(assets))
	for _, asset := range assets {
		balance := volumes[asset].GetBalance()

		balanceColor := pterm.Green
		if balance != "" && balance[0] == '-' {
			balanceColor = pterm.Red
		}

		lines = append(lines, fmt.Sprintf("%s %s", asset, balanceColor(balance)))
	}

	return lines
}
