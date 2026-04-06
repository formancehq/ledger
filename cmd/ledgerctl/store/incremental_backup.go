package store

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
)

// NewBackupCommand creates the store backup command.
func NewBackupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "backup",
		Aliases: []string{"bk"},
		Short:   "Perform an incremental backup",
		Long:    "Perform an incremental backup of the Pebble store to a filesystem path or S3 bucket",
		RunE:    runBackup,
	}

	cmd.Flags().String("driver", "", "Backup storage driver: 'filesystem' or 's3' (required)")
	cmd.Flags().String("path", "", "Base path for filesystem backup storage (required when driver=filesystem)")
	cmd.Flags().String("bucket-id", "", "Namespace prefix for backup files (default: cluster-id)")
	cmd.Flags().String("s3-bucket", "", "S3 bucket name (required when driver=s3)")
	cmd.Flags().String("s3-region", "", "AWS region for S3 bucket")
	cmd.Flags().String("s3-endpoint", "", "Custom S3 endpoint (for MinIO)")
	cmd.Flags().Duration("timeout", 10*cmdutil.DefaultTimeout, "Request timeout")
	cmdutil.AddOutputFlags(cmd)

	_ = cmd.MarkFlagRequired("driver")

	return cmd
}

func runBackup(cmd *cobra.Command, _ []string) error {
	driver, _ := cmd.Flags().GetString("driver")
	basePath, _ := cmd.Flags().GetString("path")
	bucketID, _ := cmd.Flags().GetString("bucket-id")
	s3Bucket, _ := cmd.Flags().GetString("s3-bucket")
	s3Region, _ := cmd.Flags().GetString("s3-region")
	s3Endpoint, _ := cmd.Flags().GetString("s3-endpoint")

	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	structuredOutput := cmdutil.IsStructuredOutput(cmd)

	var spinner *pterm.SpinnerPrinter
	if !structuredOutput {
		spinner, _ = pterm.DefaultSpinner.Start("Running incremental backup...")
	}

	resp, err := client.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{
		Driver:     driver,
		BasePath:   basePath,
		BucketId:   bucketID,
		S3Bucket:   s3Bucket,
		S3Region:   s3Region,
		S3Endpoint: s3Endpoint,
	})
	if err != nil {
		if spinner != nil {
			_ = spinner.Stop()
		}

		return cmdutil.FormatGRPCError("incremental backup failed", err)
	}

	if spinner != nil {
		spinner.Success(fmt.Sprintf("Incremental backup completed: %d uploaded, %d deleted, %d total (%dms)",
			resp.GetFilesUploaded(), resp.GetFilesDeleted(), resp.GetTotalFiles(), resp.GetDurationMs()))
	}

	if handled, err := cmdutil.EncodeStructured(cmd, struct {
		FilesUploaded uint32 `json:"filesUploaded"`
		FilesDeleted  uint32 `json:"filesDeleted"`
		TotalFiles    uint32 `json:"totalFiles"`
		DurationMs    int64  `json:"durationMs"`
	}{
		FilesUploaded: resp.GetFilesUploaded(),
		FilesDeleted:  resp.GetFilesDeleted(),
		TotalFiles:    resp.GetTotalFiles(),
		DurationMs:    resp.GetDurationMs(),
	}); handled || err != nil {
		return err
	}

	return nil
}
