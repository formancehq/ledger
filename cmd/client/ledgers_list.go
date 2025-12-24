package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var (
	ledgersListIncludeDeleted bool
)

var ledgersListCmd = &cobra.Command{
	Use:          "list",
	Aliases:      []string{"ls", "l"},
	Short:        "List all ledgers",
	Long:         "Returns a list of all ledgers. By default, deleted ledgers are excluded. Use --include-deleted to include them.",
	RunE:         runListLedgers,
	SilenceUsage: true,
}

func init() {
	ledgersListCmd.Flags().BoolVar(&ledgersListIncludeDeleted, "include-deleted", false, "Include deleted ledgers in the list")
}

func runListLedgers(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	spinner, _ := pterm.DefaultSpinner.Start("Fetching ledgers...")

	// Call the list all ledgers endpoint
	var includeDeletedPtr *bool
	if ledgersListIncludeDeleted {
		includeDeletedPtr = &ledgersListIncludeDeleted
	}
	res, err := sdk.Ledgers.ListAllLedgers(ctx, operations.ListAllLedgersRequest{
		IncludeDeleted: includeDeletedPtr,
	})
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
		{"ID", "Name", "Driver", "Created At", "Deleted At"},
	}

	for _, ledger := range ledgers {
		id := fmt.Sprintf("%d", ledger.ID)
		name := ledger.Name
		driver := string(ledger.Driver)
		createdAt := ledger.CreatedAt.Format("2006-01-02 15:04:05")
		deletedAt := "N/A"
		if ledger.DeletedAt != nil {
			deletedAt = ledger.DeletedAt.Format("2006-01-02 15:04:05")
		}
		tableData = append(tableData, []string{id, name, driver, createdAt, deletedAt})
	}

	pterm.DefaultSection.Println("All Ledgers")
	return pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}
