package store

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// NewIncrementalBackupCommand creates the store incremental-backup command.
func NewIncrementalBackupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "incremental-backup",
		Aliases: []string{"ibk"},
		Short:   "Export new log and audit entries since the last backup",
		Long:    "Perform an incremental backup by exporting new log and audit entries to S3. Requires a prior full backup.",
		RunE:    runIncrementalBackup,
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

func runIncrementalBackup(cmd *cobra.Command, _ []string) error {
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
		spinner, _ = pterm.DefaultSpinner.Start("Running incremental backup...")
	}

	resp, err := client.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{
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

		return cmdutil.FormatGRPCError("incremental backup failed", err)
	}

	if spinner != nil {
		spinner.Success(fmt.Sprintf("Incremental backup completed: %d log entries, %d audit entries, %d segments, %d orphans pruned (log_seq=%d, audit_seq=%d) (%dms)",
			resp.GetLogEntriesExported(), resp.GetAuditEntriesExported(), resp.GetSegmentsUploaded(), resp.GetOrphansDeleted(),
			resp.GetLastLogSequence(), resp.GetLastAuditSequence(),
			resp.GetDurationMs()))
	}

	if handled, err := cmdutil.EncodeStructured(cmd, struct {
		LogEntriesExported   uint64 `json:"logEntriesExported"`
		AuditEntriesExported uint64 `json:"auditEntriesExported"`
		SegmentsUploaded     uint32 `json:"segmentsUploaded"`
		OrphansDeleted       uint32 `json:"orphansDeleted"`
		DurationMs           int64  `json:"durationMs"`
		LastLogSequence      uint64 `json:"lastLogSequence"`
		LastAuditSequence    uint64 `json:"lastAuditSequence"`
	}{
		LogEntriesExported:   resp.GetLogEntriesExported(),
		AuditEntriesExported: resp.GetAuditEntriesExported(),
		SegmentsUploaded:     resp.GetSegmentsUploaded(),
		OrphansDeleted:       resp.GetOrphansDeleted(),
		DurationMs:           resp.GetDurationMs(),
		LastLogSequence:      resp.GetLastLogSequence(),
		LastAuditSequence:    resp.GetLastAuditSequence(),
	}); handled || err != nil {
		return err
	}

	return nil
}
