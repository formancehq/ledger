package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var getBucketName string

var bucketsGetCmd = &cobra.Command{
	Use:          "get",
	Short:        "Get a bucket",
	Long:         "Retrieves bucket information",
	RunE:         runGetBucket,
	SilenceUsage: true,
}

func init() {
	bucketsGetCmd.Flags().StringVar(&getBucketName, "name", "", "Bucket name to retrieve (required)")
	if err := bucketsGetCmd.MarkFlagRequired("name"); err != nil {
		panic(err)
	}
	bucketsGetCmd.RegisterFlagCompletionFunc("name", completeBucketNames())
}

func runGetBucket(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	if getBucketName == "" {
		return fmt.Errorf("bucket name is required")
	}

	sdk := newSDKClient()

	req := operations.GetBucketRequest{
		BucketName: getBucketName,
	}

	res, err := sdk.Buckets.GetBucket(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to get bucket: %w", err)
	}

	bucketResponse := res.GetGetBucketResponse()
	if bucketResponse == nil {
		return fmt.Errorf("no bucket data in response")
	}

	data := bucketResponse.Data

	// Bucket information panel
	bucketInfo := ""
	bucketInfo += fmt.Sprintf("ID: %d\n", data.ID)
	bucketInfo += fmt.Sprintf("Name: %s\n", data.Name)
	bucketInfo += fmt.Sprintf("Driver: %s\n", string(data.Driver))
	bucketInfo += fmt.Sprintf("Created At: %s\n", data.CreatedAt.Format("2006-01-02 15:04:05"))
	if data.SnapshotThreshold != nil && *data.SnapshotThreshold > 0 {
		bucketInfo += fmt.Sprintf("Snapshot Threshold: %d\n", *data.SnapshotThreshold)
	} else {
		bucketInfo += "Snapshot Threshold: (using global config)\n"
	}

	pterm.DefaultHeader.WithFullWidth().Println("Bucket Information")
	pterm.Println()
	pterm.DefaultBox.WithTitle("Bucket Details").WithBoxStyle(pterm.NewStyle(pterm.FgLightCyan)).Println(bucketInfo)

	return nil
}
