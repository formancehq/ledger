package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newAccountsGetCommand creates the accounts get command.
func newAccountsGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get [address]",
		Aliases: []string{"g", "show", "describe"},
		Short:   "Get an account by address",
		Long: `Get detailed information about an account including its volumes via gRPC.

If --ledger is not provided and only one ledger exists, it will be used automatically.
If multiple ledgers exist, you will be prompted to select one.

Examples:
  ledgerctl accounts get bank --ledger my-ledger
  ledgerctl accounts get bank  # Will prompt for ledger if needed
  ledgerctl accounts get       # Will prompt for both ledger and address`,
		Args: cobra.MaximumNArgs(1),
		RunE: runAccountsGet,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runAccountsGet(cmd *cobra.Command, args []string) error {
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

	// Get address (from args or prompt)
	var address string
	if len(args) > 0 {
		address = args[0]
	} else {
		result, err := pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter account address").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}
		address = result
		if address == "" {
			pterm.Error.Println("Account address is required")
			return fmt.Errorf("account address is required")
		}
	}

	ctx, cancel := getContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching account %s...", pterm.Cyan(address)))

	account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
		Ledger:  ledgerName,
		Address: address,
	})
	if err != nil {
		spinner.Fail("Failed to get account")
		return formatGRPCError("failed to get account", err)
	}

	_ = spinner.Stop()

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(account)
	}

	pterm.Println()

	// Display account header
	pterm.Printf("Account: %s\n", account.Address)
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	// Display metadata if present
	if account.Metadata != nil && len(account.Metadata.Metadata) > 0 {
		pterm.Println("Metadata:")
		metadataTable := pterm.TableData{
			{"KEY", "VALUE"},
		}
		for _, md := range account.Metadata.Metadata {
			metadataTable = append(metadataTable, []string{
				md.Key,
				md.Value.Value,
			})
		}
		if err := pterm.DefaultTable.WithHasHeader().WithData(metadataTable).Render(); err != nil {
			return err
		}
		pterm.Println()
	}

	// Display volumes
	pterm.Println("Volumes:")
	if len(account.Volumes) > 0 {
		volumesTable := pterm.TableData{
			{"ASSET", "INPUT", "OUTPUT", "BALANCE"},
		}

		// Sort assets for consistent output
		assets := make([]string, 0, len(account.Volumes))
		for asset := range account.Volumes {
			assets = append(assets, asset)
		}
		sort.Strings(assets)

		for _, asset := range assets {
			vol := account.Volumes[asset]
			balance := vol.Balance
			balanceColor := pterm.Green
			if balance != "" && balance[0] == '-' {
				balanceColor = pterm.Red
			}
			volumesTable = append(volumesTable, []string{
				asset,
				vol.Input,
				vol.Output,
				balanceColor(balance),
			})
		}
		return pterm.DefaultTable.WithHasHeader().WithData(volumesTable).Render()
	}

	pterm.Println(pterm.Gray("(no volumes)"))
	return nil
}
