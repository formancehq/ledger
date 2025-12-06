package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var (
	getLedgerBucketName string
	getLedgerName       string
)

var ledgersGetCmd = &cobra.Command{
	Use:          "get",
	Short:        "Get a ledger",
	Long:         "Retrieves a ledger from the specified bucket",
	RunE:         runGetLedger,
	SilenceUsage: true,
}

func init() {
	ledgersGetCmd.Flags().StringVar(&getLedgerBucketName, "bucket", "", "Bucket name (required)")
	ledgersGetCmd.Flags().StringVar(&getLedgerName, "name", "", "Ledger name (required)")
	ledgersGetCmd.MarkFlagRequired("bucket")
	ledgersGetCmd.MarkFlagRequired("name")
}

func runGetLedger(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Validate required flags
	if getLedgerBucketName == "" {
		return fmt.Errorf("bucket name is required (use --bucket)")
	}
	if getLedgerName == "" {
		return fmt.Errorf("ledger name is required (use --name)")
	}

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	// Get ledger request
	req := operations.GetLedgerFromBucketRequest{
		BucketName: getLedgerBucketName,
		LedgerName: getLedgerName,
	}

	// Call the get ledger endpoint
	res, err := sdk.Ledgers.GetLedgerFromBucket(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to get ledger: %w", err)
	}

	// Extract response data
	ledgerResponse := res.GetGetLedgerFromBucketResponse()
	if ledgerResponse == nil || ledgerResponse.Data == nil {
		return fmt.Errorf("no ledger data in response")
	}

	data := ledgerResponse.Data

	// Create info panel
	panelData := ""
	if data.Name != nil {
		panelData += fmt.Sprintf("Name: %s\n", *data.Name)
	}
	if data.Bucket != nil {
		panelData += fmt.Sprintf("Bucket: %s\n", *data.Bucket)
	}
	if data.CreatedAt != nil {
		panelData += fmt.Sprintf("Created At: %s\n", data.CreatedAt.Format("2006-01-02 15:04:05"))
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

