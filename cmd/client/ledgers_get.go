package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var getLedgerName string

var ledgersGetCmd = &cobra.Command{
	Use:          "get",
	Short:        "Get a ledger",
	Long:         "Retrieves a ledger by its name (bucket is found automatically)",
	RunE:         runGetLedger,
	SilenceUsage: true,
}

func init() {
	ledgersGetCmd.Flags().StringVar(&getLedgerName, "name", "", "Ledger name (required)")
	ledgersGetCmd.MarkFlagRequired("name")
}

func runGetLedger(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Validate required flags
	if getLedgerName == "" {
		return fmt.Errorf("ledger name is required (use --name)")
	}

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	// Get ledger request
	req := operations.GetLedgerRequest{
		LedgerName: getLedgerName,
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching ledger...")

	// Call the get ledger endpoint
	res, err := sdk.Ledgers.GetLedger(ctx, req)
	if err != nil {
		spinner.Fail("Failed to get ledger: " + err.Error())
		return fmt.Errorf("failed to get ledger: %w", err)
	}

	spinner.Success("Ledger retrieved successfully")

	// Extract response data
	ledgerResponse := res.GetGetLedgerResponse()
	if ledgerResponse == nil || ledgerResponse.Data == nil {
		return fmt.Errorf("no ledger data in response")
	}

	data := ledgerResponse.Data

	// Create info panel
	panelData := ""
	if data.ID != nil {
		panelData += fmt.Sprintf("ID: %d\n", *data.ID)
	}
	if data.Name != nil {
		panelData += fmt.Sprintf("Name: %s\n", *data.Name)
	}
	if data.Bucket != nil {
		panelData += fmt.Sprintf("Bucket: %s\n", *data.Bucket)
	}
	if data.CreatedAt != nil {
		panelData += fmt.Sprintf("Created At: %s\n", data.CreatedAt.Format("2006-01-02 15:04:05"))
	}
	if data.LastLogID != nil {
		panelData += fmt.Sprintf("Last Log ID: %d\n", *data.LastLogID)
	}
	if len(data.Metadata) > 0 {
		panelData += "\nMetadata:\n"
		for k, v := range data.Metadata {
			panelData += fmt.Sprintf("  %s: %s\n", k, v)
		}
	}

	pterm.DefaultHeader.WithFullWidth().Println("Ledger Information")
	pterm.Println()
	pterm.DefaultBox.WithTitle("Ledger Details").WithBoxStyle(pterm.NewStyle(pterm.FgLightGreen)).Println(panelData)

	return nil
}

