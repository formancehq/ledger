package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
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
	bucketsDeleteCmd.MarkFlagRequired("name")
}

func runDeleteBucket(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	if deleteBucketName == "" {
		return fmt.Errorf("bucket name is required")
	}

	sdk := newSDKClient()

	req := operations.DeleteBucketRequest{
		BucketName: deleteBucketName,
	}

	res, err := sdk.Buckets.DeleteBucket(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}

	deleteResponse := res.GetDeleteBucketResponse()
	if deleteResponse == nil || deleteResponse.Data == nil {
		fmt.Printf("Bucket %s deleted successfully\n", deleteBucketName)
		return nil
	}

	data := deleteResponse.Data
	if data.Message != nil {
		fmt.Println(*data.Message)
	} else {
		fmt.Printf("Bucket %s deleted successfully\n", deleteBucketName)
	}

	return nil
}

