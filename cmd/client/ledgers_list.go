package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newLedgersListCommand creates the ledgers list command.
func newLedgersListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "l"},
		Short:   "List all ledgers",
		Long:    "List all ledgers in the cluster via gRPC",
		RunE:    runLedgersList,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runLedgersList(cmd *cobra.Command, _ []string) error {
	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Fetching ledgers...")

	ledgers, err := getAllLedgersInfo(ctx, client)
	if err != nil {
		spinner.Fail("Failed to fetch ledgers")
		return fmt.Errorf("failed to list ledgers: %w", err)
	}

	_ = spinner.Stop()

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(ledgers)
	}

	// Sort ledgers by name for consistent output
	names := make([]string, 0, len(ledgers))
	for name := range ledgers {
		names = append(names, name)
	}
	sort.Strings(names)

	if len(names) == 0 {
		pterm.Println("No ledgers found.")
		pterm.Println(pterm.Gray("Create one with: ledgerctl ledgers create --name <name>"))
		return nil
	}

	// Build table data
	tableData := pterm.TableData{
		{"ID", "NAME", "CREATED AT"},
	}

	for _, name := range names {
		ledger := ledgers[name]
		createdAt := "-"
		if ledger.CreatedAt != nil {
			createdAt = ledger.CreatedAt.AsTime().Format(time.RFC3339)
		}
		tableData = append(tableData, []string{
			fmt.Sprintf("%d", ledger.Id),
			ledger.Name,
			createdAt,
		})
	}

	pterm.Println()
	return pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}
