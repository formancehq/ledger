package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var snapshotBucketName string

var bucketsSnapshotCmd = &cobra.Command{
	Use:          "snapshot",
	Short:        "Create a snapshot for a bucket",
	Long:         "Creates a snapshot of the bucket's Raft cluster state",
	RunE:         runCreateBucketSnapshot,
	SilenceUsage: true,
}

func init() {
	bucketsSnapshotCmd.Flags().StringVar(&snapshotBucketName, "name", "", "Bucket name to create snapshot for (required)")
	if err := bucketsSnapshotCmd.MarkFlagRequired("name"); err != nil {
		panic(err)
	}
	bucketsSnapshotCmd.RegisterFlagCompletionFunc("name", completeBucketNames())
}

func runCreateBucketSnapshot(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	if snapshotBucketName == "" {
		return fmt.Errorf("bucket name is required")
	}

	sdk := newSDKClient()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Creating snapshot for bucket %s...", snapshotBucketName))

	req := operations.CreateBucketSnapshotRequest{
		BucketName: snapshotBucketName,
	}

	res, err := sdk.Buckets.CreateBucketSnapshot(ctx, req)
	if err != nil {
		spinner.Fail(fmt.Sprintf("Failed to create snapshot for bucket %s", snapshotBucketName))
		return fmt.Errorf("failed to create bucket snapshot: %w", err)
	}

	snapshotResponse := res.GetCreateBucketSnapshotResponse()
	message := fmt.Sprintf("Snapshot created successfully for bucket %s", snapshotBucketName)
	if snapshotResponse != nil && snapshotResponse.Message != nil {
		message = *snapshotResponse.Message
	}

	spinner.Success(message)
	return nil
}
