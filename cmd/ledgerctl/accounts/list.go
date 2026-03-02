package accounts

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/filterexpr"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewListCommand creates the accounts list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "l"},
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
  ledgerctl accounts list --all   # Fetch all accounts without pagination`,
		RunE: runList,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Uint32("page-size", cmdutil.DefaultPageSize, "Number of accounts per page")
	cmd.Flags().String("prefix", "", "Filter accounts by address prefix (e.g. users:)")
	cmd.Flags().String("filter", "", `Filter expression (e.g. "metadata[category] == premium or address ^= users:")`)
	cmd.Flags().Bool("reverse", false, "Reverse iteration order (Z→A instead of A→Z)")
	cmd.Flags().Bool("all", false, "Fetch all accounts at once (no pagination)")
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Uint64("min-log-sequence", 0, "Minimum log sequence the server must have applied before reading (0 = no constraint)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmd.Flags().Bool("profile", false, "Display query execution profile (iterator stats, timing)")

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

	pageSize, _ := cmd.Flags().GetUint32("page-size")
	prefix, _ := cmd.Flags().GetString("prefix")
	filterExpr, _ := cmd.Flags().GetString("filter")
	reverse, _ := cmd.Flags().GetBool("reverse")
	fetchAll, _ := cmd.Flags().GetBool("all")
	jsonOutput, _ := cmd.Flags().GetBool("json")
	minLogSeq, _ := cmd.Flags().GetUint64("min-log-sequence")
	showProfile, _ := cmd.Flags().GetBool("profile")

	// Build the filter from --filter and --prefix flags
	filter, err := buildAccountFilter(filterExpr, prefix)
	if err != nil {
		return err
	}

	if fetchAll {
		return fetchAllAccounts(cmd, client, ledgerName, filter, reverse, jsonOutput, minLogSeq, showProfile)
	}

	return fetchAccountsWithPager(cmd, client, ledgerName, pageSize, filter, reverse, jsonOutput, minLogSeq, showProfile)
}

// buildAccountFilter combines --filter and --prefix flags into a single QueryFilter.
func buildAccountFilter(filterExpr, prefix string) (*commonpb.QueryFilter, error) {
	var parsedFilter *commonpb.QueryFilter
	if filterExpr != "" {
		var err error
		parsedFilter, err = filterexpr.Parse(filterExpr)
		if err != nil {
			return nil, fmt.Errorf("invalid filter expression: %w", err)
		}
	}

	var prefixFilter *commonpb.QueryFilter
	if prefix != "" {
		prefixFilter = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: prefix},
				},
			},
		}
	}

	switch {
	case parsedFilter != nil && prefixFilter != nil:
		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{prefixFilter, parsedFilter}},
			},
		}, nil
	case parsedFilter != nil:
		return parsedFilter, nil
	case prefixFilter != nil:
		return prefixFilter, nil
	default:
		return nil, nil
	}
}

func fetchAllAccounts(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, filter *commonpb.QueryFilter, reverse bool, jsonOutput bool, minLogSeq uint64, showProfile bool) error {
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	if showProfile {
		ctx = cmdutil.ProfileContext(ctx)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching all accounts...")

	stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
		Ledger:         ledgerName,
		PageSize:       0,
		Filter:         filter,
		Reverse:        reverse,
		MinLogSequence: minLogSeq,
	})
	if err != nil {
		spinner.Fail("Failed to list accounts")
		return cmdutil.FormatGRPCError("failed to list accounts", err)
	}

	var accounts []*commonpb.Account
	for {
		account, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			spinner.Fail("Failed to receive account")
			return cmdutil.FormatGRPCError("failed to receive account", err)
		}
		accounts = append(accounts, account)
	}

	_ = spinner.Stop()

	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(accounts)
	}

	if len(accounts) == 0 {
		pterm.Info.Println("No accounts found.")
		pterm.Println(pterm.Gray("Create transactions to populate accounts."))
		return nil
	}

	renderAccountsTable(accounts)

	if showProfile {
		cmdutil.RenderProfile(cmdutil.ExtractProfile(stream.Trailer()))
	}
	return nil
}

func fetchAccountsWithPager(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, pageSize uint32, filter *commonpb.QueryFilter, reverse bool, jsonOutput bool, minLogSeq uint64, showProfile bool) error {
	var afterAddress string
	pageNum := 1

	for {
		ctx, cancel := cmdutil.GetContext(cmd)
		if showProfile {
			ctx = cmdutil.ProfileContext(ctx)
		}

		spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching page %d...", pageNum))

		stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
			Ledger:         ledgerName,
			PageSize:       pageSize,
			AfterAddress:   afterAddress,
			Filter:         filter,
			Reverse:        reverse,
			MinLogSequence: minLogSeq,
		})
		if err != nil {
			cancel()
			spinner.Fail("Failed to list accounts")
			return cmdutil.FormatGRPCError("failed to list accounts", err)
		}

		var accounts []*commonpb.Account
		for {
			account, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				cancel()
				spinner.Fail("Failed to receive account")
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
			return nil
		}

		_ = spinner.Stop()

		if jsonOutput {
			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			if err := encoder.Encode(accounts); err != nil {
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

		if uint32(len(accounts)) < pageSize {
			if !jsonOutput {
				pterm.Info.Println("End of accounts.")
			}
			return nil
		}

		afterAddress = accounts[len(accounts)-1].Address

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
			return nil
		}

		pageNum++
	}
}

func renderAccountsTable(accounts []*commonpb.Account) {
	termWidth := pterm.GetTerminalWidth()

	// Reserve space for METADATA column (header is 8 chars), separator (3 spaces),
	// and continuation indent (2 spaces).
	const (
		metadataColWidth    = 8
		separatorWidth      = 3
		continuationIndent  = "  "
	)

	maxAddressWidth := termWidth - metadataColWidth - separatorWidth - len(continuationIndent)
	if maxAddressWidth < 20 {
		maxAddressWidth = 20
	}

	tableData := pterm.TableData{
		{"ADDRESS", "METADATA"},
	}

	for _, account := range accounts {
		metadataCount := "0"
		if account.Metadata != nil {
			metadataCount = fmt.Sprintf("%d", len(account.Metadata.Metadata))
		}

		lines := cmdutil.WrapText(account.Address, maxAddressWidth, ":")
		tableData = append(tableData, []string{lines[0], metadataCount})
		for _, line := range lines[1:] {
			tableData = append(tableData, []string{continuationIndent + line, ""})
		}
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}
