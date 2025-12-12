package main

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var ledgersListCmd = &cobra.Command{
	Use:          "list",
	Short:        "List all ledgers across all buckets",
	Long:         "Returns a list of all ledgers from all buckets",
	RunE:         runListLedgers,
	SilenceUsage: true,
}

func runListLedgers(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	spinner, _ := pterm.DefaultSpinner.Start("Fetching ledgers...")

	// Call the list all ledgers endpoint
	res, err := sdk.Ledgers.ListAllLedgers(ctx)
	if err != nil {
		spinner.Fail("Failed to list ledgers: " + err.Error())
		return fmt.Errorf("failed to list ledgers: %w", err)
	}
	spinner.Success("Ledgers retrieved successfully")

	// Extract response data
	ledgersResponse := res.GetListAllLedgersResponse()
	if ledgersResponse == nil || ledgersResponse.Data == nil {
		pterm.Info.Println("No ledgers found")
		return nil
	}

	ledgers := ledgersResponse.Data
	if len(ledgers) == 0 {
		pterm.Info.Println("No ledgers found")
		return nil
	}

	// Create table data
	tableData := pterm.TableData{
		{"ID", "Name", "Bucket", "Created At", "Last Log ID"},
	}

	for _, ledger := range ledgers {
		id := fmt.Sprintf("%d", ledger.ID)
		name := ledger.Name
		bucket := ledger.Bucket
		createdAt := ledger.CreatedAt.Format("2006-01-02 15:04:05")
		lastLogID := "N/A"
		if ledger.LastLogID != nil {
			lastLogID = fmt.Sprintf("%d", *ledger.LastLogID)
		}
		tableData = append(tableData, []string{id, name, bucket, createdAt, lastLogID})
	}

	pterm.DefaultSection.Println("All Ledgers")
	return pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}
