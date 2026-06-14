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
		Long:    "Perform an incremental backup by exporting new log and audit entries to S3 or Azure Blob Storage. Requires a prior full backup.",
		RunE:    runIncrementalBackup,
	}

	cmd.Flags().String("path", "", "Reserved for future use")
	cmd.Flags().String("bucket-id", "", "Namespace prefix for backup files (default: cluster-id)")
	cmdutil.AddBackupStorageFlags(cmd)
	cmd.Flags().Duration("timeout", 10*cmdutil.DefaultTimeout, "Request timeout")
	cmdutil.AddOutputFlags(cmd)

	return cmd
}

func runIncrementalBackup(cmd *cobra.Command, _ []string) error {
	basePath, _ := cmd.Flags().GetString("path")
	bucketID, _ := cmd.Flags().GetString("bucket-id")

	storage, err := cmdutil.BackupStorageFromFlags(cmd)
	if err != nil {
		return err
	}

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
		Storage:  storage,
		BasePath: basePath,
		BucketId: bucketID,
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
