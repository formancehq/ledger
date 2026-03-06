package restore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/restorepb"
)

const uploadChunkSize = 64 * 1024 // 64KB

// NewUploadCommand creates the restore upload command.
func NewUploadCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upload",
		Short: "Upload a backup tar archive",
		Long:  "Stream a tar archive backup to a server running in --restore mode",
		RunE:  runUpload,
	}

	cmd.Flags().StringP("input", "i", "", "Input tar file path (required)")
	_ = cmd.MarkFlagRequired("input")
	cmd.Flags().Duration("timeout", 10*cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runUpload(cmd *cobra.Command, _ []string) error {
	inputPath, _ := cmd.Flags().GetString("input")

	client, conn, err := getRestoreClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	// Open input file
	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("opening input file: %w", err)
	}

	defer func() { _ = f.Close() }()

	// Get file size for progress
	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("getting file info: %w", err)
	}

	totalSize := uint64(stat.Size())

	stream, err := client.UploadBackup(ctx)
	if err != nil {
		return cmdutil.FormatGRPCError("failed to start upload", err)
	}

	var (
		buf        = make([]byte, uploadChunkSize)
		hash       = sha256.New()
		totalSent  uint64
		spinner, _ = pterm.DefaultSpinner.Start("Uploading backup...")
	)

	for {
		n, err := f.Read(buf)
		if n > 0 {
			if _, err := hash.Write(buf[:n]); err != nil {
				spinner.Fail("Failed to upload backup")

				return cmdutil.Displayed(fmt.Errorf("computing hash: %w", err))
			}

			err := stream.Send(&restorepb.UploadBackupRequest{
				Data: buf[:n],
			})
			if err != nil {
				_ = spinner.Stop()

				return cmdutil.FormatGRPCError("sending upload chunk", err)
			}

			totalSent += uint64(n)

			if totalSize > 0 {
				pct := float64(totalSent) / float64(totalSize) * 100
				spinner.UpdateText(fmt.Sprintf("Uploading backup... %s / %s (%.0f%%)",
					cmdutil.FormatBytes(totalSent), cmdutil.FormatBytes(totalSize), pct))
			}
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			spinner.Fail("Failed to upload backup")

			return cmdutil.Displayed(fmt.Errorf("reading input file: %w", err))
		}
	}

	// Send EOF with hash
	contentHash := hex.EncodeToString(hash.Sum(nil))
	if err := stream.Send(&restorepb.UploadBackupRequest{
		Eof:           true,
		ContentSha256: contentHash,
		ContentSize:   totalSent,
	}); err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("sending EOF", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("completing upload", err)
	}

	_ = spinner.Stop()

	pterm.Success.Printfln("Backup uploaded successfully (%s, SHA256: %s)",
		cmdutil.FormatBytes(resp.GetBytesReceived()), resp.GetSha256()[:12]+"...")

	return nil
}
