package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newAccountsListCommand creates the accounts list command.
func newAccountsListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "l"},
		Short:   "List accounts in a ledger",
		Long: `List accounts in a ledger via gRPC with pagination.

Accounts are displayed in alphabetical order, with interactive pagination.
Press Enter to load the next page, or 'q' to quit.

If --ledger is not provided and only one ledger exists, it will be used automatically.
If multiple ledgers exist, you will be prompted to select one.

Examples:
  ledgerctl accounts list --ledger my-ledger
  ledgerctl accounts list --ledger my-ledger --page-size 20
  ledgerctl accounts list --ledger my-ledger --prefix users:
  ledgerctl accounts list --all   # Fetch all accounts without pagination`,
		RunE: runAccountsList,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Uint32("page-size", defaultPageSize, "Number of accounts per page")
	cmd.Flags().String("prefix", "", "Filter accounts by address prefix (e.g. users:)")
	cmd.Flags().Bool("all", false, "Fetch all accounts at once (no pagination)")
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runAccountsList(cmd *cobra.Command, _ []string) error {
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
	prefix, _ := cmd.Flags().GetString("prefix")
	fetchAll, _ := cmd.Flags().GetBool("all")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	if fetchAll {
		return fetchAllAccounts(cmd, client, ledgerName, prefix, jsonOutput)
	}

	return fetchAccountsWithPager(cmd, client, ledgerName, pageSize, prefix, jsonOutput)
}

func fetchAllAccounts(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, prefix string, jsonOutput bool) error {
	ctx, cancel := getContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Fetching all accounts...")

	stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
		Ledger:   ledgerName,
		PageSize: 0, // No limit
		Prefix:   prefix,
	})
	if err != nil {
		spinner.Fail("Failed to list accounts")
		return formatGRPCError("failed to list accounts", err)
	}

	var accounts []*commonpb.Account
	for {
		account, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			spinner.Fail("Failed to receive account")
			return formatGRPCError("failed to receive account", err)
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
		pterm.Println("No accounts found in this ledger.")
		pterm.Println(pterm.Gray("Create transactions to populate accounts."))
		return nil
	}

	renderAccountsTable(accounts)
	return nil
}

func fetchAccountsWithPager(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerName string, pageSize uint32, prefix string, jsonOutput bool) error {
	var afterAddress string
	pageNum := 1

	for {
		ctx, cancel := getContext(cmd)

		spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching page %d...", pageNum))

		stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
			Ledger:       ledgerName,
			PageSize:     pageSize,
			AfterAddress: afterAddress,
			Prefix:       prefix,
		})
		if err != nil {
			cancel()
			spinner.Fail("Failed to list accounts")
			return formatGRPCError("failed to list accounts", err)
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
				return formatGRPCError("failed to receive account", err)
			}
			accounts = append(accounts, account)
		}

		cancel()

		if len(accounts) == 0 {
			spinner.Info("No more accounts.")
			if pageNum == 1 {
				pterm.Println("No accounts found in this ledger.")
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

		// If we got fewer accounts than pageSize, we've reached the end
		if uint32(len(accounts)) < pageSize {
			if !jsonOutput {
				pterm.Info.Println("End of accounts.")
			}
			return nil
		}

		// Update afterAddress for next page (last account in current page)
		afterAddress = accounts[len(accounts)-1].Address

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

func renderAccountsTable(accounts []*commonpb.Account) {
	tableData := pterm.TableData{
		{"ADDRESS", "METADATA"},
	}

	for _, account := range accounts {
		metadataCount := "0"
		if account.Metadata != nil {
			metadataCount = fmt.Sprintf("%d", len(account.Metadata.Metadata))
		}

		tableData = append(tableData, []string{
			account.Address,
			metadataCount,
		})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}
