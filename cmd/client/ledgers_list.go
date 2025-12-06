package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var listLedgerBucketName string

var ledgersListCmd = &cobra.Command{
	Use:          "list",
	Short:        "List all ledgers in a bucket",
	Long:         "Returns a list of all ledgers in the specified bucket",
	RunE:         runListLedgers,
	SilenceUsage: true,
}

func init() {
	ledgersListCmd.Flags().StringVar(&listLedgerBucketName, "bucket", "", "Bucket name (required)")
	ledgersListCmd.MarkFlagRequired("bucket")
}

func runListLedgers(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Validate required flag
	if listLedgerBucketName == "" {
		return fmt.Errorf("bucket name is required (use --bucket)")
	}

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	// List ledgers request
	req := operations.ListLedgersInBucketRequest{
		BucketName: listLedgerBucketName,
	}

	// Call the list ledgers endpoint
	res, err := sdk.Ledgers.ListLedgersInBucket(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to list ledgers: %w", err)
	}

	// Extract response data
	ledgersResponse := res.GetListLedgersInBucketResponse()
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
		{"Name", "Bucket", "Created At"},
	}

	for _, ledger := range ledgers {
		name := "N/A"
		if ledger.Name != nil {
			name = *ledger.Name
		}
		bucket := "N/A"
		if ledger.Bucket != nil {
			bucket = *ledger.Bucket
		}
		createdAt := "N/A"
		if ledger.CreatedAt != nil {
			createdAt = ledger.CreatedAt.Format("2006-01-02 15:04:05")
		}
		tableData = append(tableData, []string{name, bucket, createdAt})
	}

	pterm.DefaultHeader.WithFullWidth().Println(fmt.Sprintf("Ledgers in bucket: %s", listLedgerBucketName))
	pterm.Println()
	pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

	return nil
}

