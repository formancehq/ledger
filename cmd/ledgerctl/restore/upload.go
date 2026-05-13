package restore

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/restorepb"
)

// NewDownloadCommand creates the restore download command.
func NewDownloadCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "download",
		Short: "Download a backup from S3",
		Long:  "Download backup files from S3 into a server running in --restore mode",
		RunE:  runDownload,
	}

	cmd.Flags().String("s3-bucket", "", "S3 bucket containing the backup (required)")
	cmd.Flags().String("s3-region", "", "AWS region for S3 bucket")
	cmd.Flags().String("s3-endpoint", "", "Custom S3 endpoint (for MinIO)")
	cmd.Flags().String("s3-access-key-id", "", "Static AWS access key ID (default: use default credential chain)")
	cmd.Flags().String("s3-secret-access-key", "", "Static AWS secret access key (default: use default credential chain)")
	cmd.Flags().String("bucket-id", "", "Namespace prefix for backup files (default: cluster-id)")
	cmd.Flags().Duration("timeout", 10*cmdutil.DefaultTimeout, "Request timeout")

	_ = cmd.MarkFlagRequired("s3-bucket")

	return cmd
}

func runDownload(cmd *cobra.Command, _ []string) error {
	s3Bucket, _ := cmd.Flags().GetString("s3-bucket")
	s3Region, _ := cmd.Flags().GetString("s3-region")
	s3Endpoint, _ := cmd.Flags().GetString("s3-endpoint")
	s3AccessKeyID, _ := cmd.Flags().GetString("s3-access-key-id")
	s3SecretAccessKey, _ := cmd.Flags().GetString("s3-secret-access-key")
	bucketID, _ := cmd.Flags().GetString("bucket-id")

	client, conn, err := getRestoreClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Downloading backup from S3...")

	resp, err := client.DownloadBackup(ctx, &restorepb.DownloadBackupRequest{
		S3Bucket:          s3Bucket,
		S3Region:          s3Region,
		S3Endpoint:        s3Endpoint,
		BucketId:          bucketID,
		S3AccessKeyId:     s3AccessKeyID,
		S3SecretAccessKey: s3SecretAccessKey,
	})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("download failed", err)
	}

	spinner.Success(fmt.Sprintf("Backup downloaded: %d files, %s",
		resp.GetFilesDownloaded(), cmdutil.FormatBytes(resp.GetTotalBytes())))

	return nil
}
