package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
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

	// Print ledger information
	fmt.Println("Ledger Information")
	fmt.Println("==================")
	if data.Name != nil {
		fmt.Printf("Name: %s\n", *data.Name)
	}
	if data.Bucket != nil {
		fmt.Printf("Bucket: %s\n", *data.Bucket)
	}
	if data.CreatedAt != nil {
		fmt.Printf("Created At: %s\n", data.CreatedAt.Format("2006-01-02 15:04:05"))
	}
	if len(data.Metadata) > 0 {
		fmt.Println()
		fmt.Println("Metadata:")
		for k, v := range data.Metadata {
			fmt.Printf("  %s: %s\n", k, v)
		}
	}

	return nil
}

