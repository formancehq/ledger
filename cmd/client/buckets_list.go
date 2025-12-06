package main

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var bucketsListCmd = &cobra.Command{
	Use:          "list",
	Short:        "List all buckets",
	Long:         "Returns a list of all buckets in the cluster",
	RunE:         runListBuckets,
	SilenceUsage: true,
}

func runListBuckets(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	sdk := newSDKClient()

	res, err := sdk.Buckets.ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("failed to list buckets: %w", err)
	}

	bucketsResponse := res.GetListBucketsResponse()
	if bucketsResponse == nil || bucketsResponse.Data == nil {
		pterm.Info.Println("No buckets found")
		return nil
	}

	buckets := bucketsResponse.Data
	if len(buckets) == 0 {
		pterm.Info.Println("No buckets found")
		return nil
	}

	// Create table data
	tableData := pterm.TableData{
		{"ID", "Name", "Driver", "Created At"},
	}

	for _, bucket := range buckets {
		id := "N/A"
		if bucket.ID != nil {
			id = fmt.Sprintf("%d", *bucket.ID)
		}
		name := "N/A"
		if bucket.Name != nil {
			name = *bucket.Name
		}
		driver := "N/A"
		if bucket.Driver != nil {
			driver = *bucket.Driver
		}
		createdAt := "N/A"
		if bucket.CreatedAt != nil {
			createdAt = bucket.CreatedAt.Format("2006-01-02 15:04:05")
		}
		tableData = append(tableData, []string{id, name, driver, createdAt})
	}

	pterm.DefaultHeader.WithFullWidth().Println("Buckets")
	pterm.Println()
	pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

	return nil
}

