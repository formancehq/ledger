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
		Use:               "backup",
		Aliases:           []string{"bk"},
		Short:             "Perform a backup",
		Long:              "Perform a backup of the Pebble store to a filesystem path, S3 bucket, or Azure Blob Storage container",
		RunE:              runBackup,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("path", "", "Reserved for future use")
	cmd.Flags().String("bucket-id", "", "Namespace prefix for backup files (default: cluster-id)")
	cmdutil.AddBackupStorageFlags(cmd)
	cmd.Flags().Duration("timeout", 10*cmdutil.DefaultTimeout, "Request timeout")
	cmdutil.AddOutputFlags(cmd)

	return cmd
}

func runBackup(cmd *cobra.Command, _ []string) error {
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
		spinner, _ = pterm.DefaultSpinner.Start("Running backup...")
	}

	resp, err := client.Backup(ctx, &clusterpb.BackupRequest{
		Storage:  storage,
		BasePath: basePath,
		BucketId: bucketID,
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
