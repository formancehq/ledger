package main

import (
	"encoding/json"
	"fmt"

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
		fmt.Println("No buckets found")
		return nil
	}

	buckets := bucketsResponse.Data
	if len(buckets) == 0 {
		fmt.Println("No buckets found")
		return nil
	}

	fmt.Println("Buckets:")
	fmt.Println("========")
	for i, bucket := range buckets {
		fmt.Printf("\n%d. ", i+1)
		if bucket.ID != nil {
			fmt.Printf("ID: %d\n", *bucket.ID)
		}
		if bucket.Name != nil {
			fmt.Printf("   Name: %s\n", *bucket.Name)
		}
		if bucket.Driver != nil {
			fmt.Printf("   Driver: %s\n", *bucket.Driver)
		}
		if bucket.CreatedAt != nil {
			fmt.Printf("   Created At: %s\n", *bucket.CreatedAt)
		}
		if bucket.Config != nil {
			configJSON, err := json.MarshalIndent(bucket.Config, "   ", "  ")
			if err == nil {
				fmt.Printf("   Config: %s\n", string(configJSON))
			}
		}
	}

	return nil
}

