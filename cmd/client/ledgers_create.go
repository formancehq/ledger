package main

import (
	"encoding/json"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var (
	ledgerBucketName string
	ledgerName       string
	ledgerMetadata   string
)

var ledgersCreateCmd = &cobra.Command{
	Use:          "create",
	Short:        "Create a new ledger",
	Long:         "Creates a new ledger in the specified bucket",
	RunE:         runCreateLedger,
	SilenceUsage: true,
}

func init() {
	ledgersCreateCmd.Flags().StringVar(&ledgerBucketName, "bucket", "", "Bucket name (required)")
	ledgersCreateCmd.Flags().StringVar(&ledgerName, "name", "", "Ledger name (required)")
	ledgersCreateCmd.Flags().StringVar(&ledgerMetadata, "metadata", "{}", "Metadata as JSON (default: {})")
	ledgersCreateCmd.MarkFlagRequired("bucket")
	ledgersCreateCmd.MarkFlagRequired("name")
}

func runCreateLedger(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Validate required flags
	if ledgerBucketName == "" {
		return fmt.Errorf("bucket name is required (use --bucket)")
	}
	if ledgerName == "" {
		return fmt.Errorf("ledger name is required (use --name)")
	}

	// Parse metadata JSON
	var metadata map[string]string
	if err := json.Unmarshal([]byte(ledgerMetadata), &metadata); err != nil {
		return fmt.Errorf("invalid metadata JSON: %w", err)
	}

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	// Create ledger request
	req := operations.CreateLedgerInBucketRequest{
		BucketName: ledgerBucketName,
		LedgerName: ledgerName,
		CreateLedgerRequest: &components.CreateLedgerRequest{
			Metadata: metadata,
		},
	}

	// Show spinner while creating
	spinner, _ := pterm.DefaultSpinner.Start("Creating ledger...")

	// Call the create ledger endpoint
	res, err := sdk.Ledgers.CreateLedgerInBucket(ctx, req)
	if err != nil {
		spinner.Fail("Failed to create ledger")
		return fmt.Errorf("failed to create ledger: %w", err)
	}

	// Extract response data
	ledgerResponse := res.GetCreateLedgerInBucketResponse()
	if ledgerResponse == nil || ledgerResponse.Data == nil {
		spinner.Success("Ledger created successfully")
		return nil
	}

	data := ledgerResponse.Data
	spinner.Success("Ledger created successfully")
	pterm.Println()

	// Create info panel
	panelData := ""
	if data.Name != nil {
		panelData += fmt.Sprintf("Name: %s\n", *data.Name)
	}
	if data.Bucket != nil {
		panelData += fmt.Sprintf("Bucket: %s\n", *data.Bucket)
	}
	if len(data.Metadata) > 0 {
		panelData += "Metadata:\n"
		for k, v := range data.Metadata {
			panelData += fmt.Sprintf("  %s: %s\n", k, v)
		}
	}

	pterm.DefaultBox.WithTitle("Ledger Information").WithBoxStyle(pterm.NewStyle(pterm.FgLightGreen)).Println(panelData)

	return nil
}

