package main

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// deleteBucketOptions holds the options for the delete bucket command
type deleteBucketOptions struct {
	name string
}

var bucketsDeleteCmd = &cobra.Command{
	Use:          "delete",
	Short:        "Delete a bucket",
	Long:         "Deletes a bucket with the specified name",
	RunE:         runDeleteBucket,
	SilenceUsage: true,
}

func init() {
	bucketsDeleteCmd.Flags().String("name", "", "Bucket name to delete")
	// Name is no longer required - wizard will prompt if not provided
	bucketsDeleteCmd.RegisterFlagCompletionFunc("name", completeBucketNames())
}

func runDeleteBucket(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Extract options from flags
	opts := &deleteBucketOptions{}
	opts.name, _ = cmd.Flags().GetString("name")

	// Create SDK instance once
	sdk := newSDKClient()

	// Run wizard if name not provided
	if opts.name == "" {
		if err := runDeleteBucketWizard(ctx, sdk, opts); err != nil {
			return err
		}
	}

	// Validate required fields after wizard
	if opts.name == "" {
		return fmt.Errorf("bucket name is required")
	}

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Deleting bucket %s...", opts.name))

	req := operations.DeleteBucketRequest{
		BucketName: opts.name,
	}

	res, err := sdk.Buckets.DeleteBucket(ctx, req)
	if err != nil {
		spinner.Fail(fmt.Sprintf("Failed to delete bucket %s", opts.name))
		return fmt.Errorf("failed to delete bucket: %w", err)
	}

	deleteResponse := res.GetDeleteBucketResponse()
	message := fmt.Sprintf("Bucket %s deleted successfully", opts.name)
	if deleteResponse != nil && deleteResponse.Data != nil && deleteResponse.Data.Message != nil {
		message = *deleteResponse.Data.Message
	}

	spinner.Success(message)
	return nil
}

// runDeleteBucketWizard runs an interactive wizard to select a bucket to delete
func runDeleteBucketWizard(ctx context.Context, sdk *client.Formance, opts *deleteBucketOptions) error {
	pterm.DefaultHeader.WithFullWidth().Println("Bucket Deletion Wizard")
	pterm.Println()

	// Fetch available buckets
	res, err := sdk.Buckets.ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("failed to list buckets: %w", err)
	}

	bucketsResponse := res.GetListBucketsResponse()
	if bucketsResponse == nil || bucketsResponse.Data == nil || len(bucketsResponse.Data) == 0 {
		return fmt.Errorf("no buckets available to delete")
	}

	buckets := bucketsResponse.Data

	// Build options list for selection
	options := make([]string, 0, len(buckets))
	bucketMap := make(map[string]string) // Maps display string to bucket name

	for _, bucket := range buckets {
		bucketName := "N/A"
		if bucket.Name != nil {
			bucketName = *bucket.Name
		}
		driver := "N/A"
		if bucket.Driver != nil {
			driver = *bucket.Driver
		}
		displayName := fmt.Sprintf("%s (%s)", bucketName, driver)
		options = append(options, displayName)
		bucketMap[displayName] = bucketName
	}

	selectedOption, err := pterm.DefaultInteractiveSelect.
		WithOptions(options).
		Show("Select a bucket to delete")
	if err != nil {
		return fmt.Errorf("failed to select bucket: %w", err)
	}

	opts.name = bucketMap[selectedOption]
	if opts.name == "" {
		return fmt.Errorf("failed to parse bucket from selection")
	}

	pterm.Warning.Printf("You are about to delete bucket: %s\n", opts.name)
	pterm.Println()

	// Confirm deletion
	confirm, err := pterm.DefaultInteractiveConfirm.
		WithDefaultValue(false).
		Show("Are you sure you want to delete this bucket?")
	if err != nil {
		return fmt.Errorf("failed to get confirmation: %w", err)
	}

	if !confirm {
		return fmt.Errorf("deletion cancelled")
	}

	pterm.Println()
	return nil
}
