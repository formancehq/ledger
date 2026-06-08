package store

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// NewBackupCommand creates the store backup command.
func NewBackupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "backup",
		Aliases: []string{"bk"},
		Short:   "Perform an backup",
		Long:    "Perform an backup of the Pebble store to a filesystem path or S3 bucket",
		RunE:    runBackup,
	}

	cmd.Flags().String("driver", "s3", "Backup storage driver (only 's3' is supported)")
	cmd.Flags().String("path", "", "Reserved for future use")
	cmd.Flags().String("bucket-id", "", "Namespace prefix for backup files (default: cluster-id)")
	cmd.Flags().String("s3-bucket", "", "S3 bucket name (required when driver=s3)")
	cmd.Flags().String("s3-region", "", "AWS region for S3 bucket")
	cmd.Flags().String("s3-endpoint", "", "Custom S3 endpoint (for MinIO)")
	cmd.Flags().String("s3-access-key-id", "", "Static AWS access key ID (default: use default credential chain)")
	cmd.Flags().String("s3-secret-access-key", "", "Static AWS secret access key (default: use default credential chain)")
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
	s3AccessKeyID, _ := cmd.Flags().GetString("s3-access-key-id")
	s3SecretAccessKey, _ := cmd.Flags().GetString("s3-secret-access-key")

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
		spinner, _ = pterm.DefaultSpinner.Start("Running backup...")
	}

	resp, err := client.Backup(ctx, &clusterpb.BackupRequest{
		Driver:            driver,
		BasePath:          basePath,
		BucketId:          bucketID,
		S3Bucket:          s3Bucket,
		S3Region:          s3Region,
		S3Endpoint:        s3Endpoint,
		S3AccessKeyId:     s3AccessKeyID,
		S3SecretAccessKey: s3SecretAccessKey,
	})
	if err != nil {
		if spinner != nil {
			_ = spinner.Stop()
		}

		return cmdutil.FormatGRPCError("backup failed", err)
	}

	if spinner != nil {
		spinner.Success(fmt.Sprintf("Backup completed: %d uploaded, %d deleted, %d orphans pruned, %d total (log_seq=%d, audit_seq=%d, applied_idx=%d) (%dms)",
			resp.GetFilesUploaded(), resp.GetFilesDeleted(), resp.GetOrphansDeleted(), resp.GetTotalFiles(),
			resp.GetLastLogSequence(), resp.GetLastAuditSequence(), resp.GetLastAppliedIndex(),
			resp.GetDurationMs()))
	}

	if handled, err := cmdutil.EncodeStructured(cmd, struct {
		FilesUploaded     uint32 `json:"filesUploaded"`
		FilesDeleted      uint32 `json:"filesDeleted"`
		OrphansDeleted    uint32 `json:"orphansDeleted"`
		TotalFiles        uint32 `json:"totalFiles"`
		DurationMs        int64  `json:"durationMs"`
		LastLogSequence   uint64 `json:"lastLogSequence"`
		LastAuditSequence uint64 `json:"lastAuditSequence"`
		LastAppliedIndex  uint64 `json:"lastAppliedIndex"`
	}{
		FilesUploaded:     resp.GetFilesUploaded(),
		FilesDeleted:      resp.GetFilesDeleted(),
		OrphansDeleted:    resp.GetOrphansDeleted(),
		TotalFiles:        resp.GetTotalFiles(),
		DurationMs:        resp.GetDurationMs(),
		LastLogSequence:   resp.GetLastLogSequence(),
		LastAuditSequence: resp.GetLastAuditSequence(),
		LastAppliedIndex:  resp.GetLastAppliedIndex(),
	}); handled || err != nil {
		return err
	}

	return nil
}
