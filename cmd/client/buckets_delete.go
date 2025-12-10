package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var deleteBucketName string

var bucketsDeleteCmd = &cobra.Command{
	Use:          "delete",
	Short:        "Delete a bucket",
	Long:         "Deletes a bucket with the specified name",
	RunE:         runDeleteBucket,
	SilenceUsage: true,
}

func init() {
	bucketsDeleteCmd.Flags().StringVar(&deleteBucketName, "name", "", "Bucket name to delete (required)")
	if err := bucketsDeleteCmd.MarkFlagRequired("name"); err != nil {
		panic(err)
	}
}

func runDeleteBucket(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	if deleteBucketName == "" {
		return fmt.Errorf("bucket name is required")
	}

	sdk := newSDKClient()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Deleting bucket %s...", deleteBucketName))

	req := operations.DeleteBucketRequest{
		BucketName: deleteBucketName,
	}

	res, err := sdk.Buckets.DeleteBucket(ctx, req)
	if err != nil {
		spinner.Fail(fmt.Sprintf("Failed to delete bucket %s", deleteBucketName))
		return fmt.Errorf("failed to delete bucket: %w", err)
	}

	deleteResponse := res.GetDeleteBucketResponse()
	message := fmt.Sprintf("Bucket %s deleted successfully", deleteBucketName)
	if deleteResponse != nil && deleteResponse.Data != nil && deleteResponse.Data.Message != nil {
		message = *deleteResponse.Data.Message
	}

	spinner.Success(message)
	return nil
}
