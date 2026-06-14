package restore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

// defaultPollInterval is the spacing between GetDownloadStatus calls during a
// running download. 3 s is short enough to feel responsive in pterm without
// generating meaningful load against the server.
const defaultPollInterval = 3 * time.Second

// defaultRPCTimeout caps each individual RPC (Start, status poll, Cancel). It
// is intentionally short: a stuck call should be observed within seconds and
// retried on the next poll tick, regardless of how long the overall download
// takes.
const defaultRPCTimeout = 30 * time.Second

// NewDownloadCommand creates the restore download command.
func NewDownloadCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "download",
		Short: "Download a backup from S3",
		Long: "Download backup files from S3 into a server running in --restore mode.\n" +
			"The download runs asynchronously on the server and survives ingress/load-balancer\n" +
			"timeouts. Use Ctrl+C to cancel a running download cleanly.",
		RunE: runDownload,
	}

	cmdutil.AddBackupStorageFlags(cmd)
	cmd.Flags().String("bucket-id", "", "Namespace prefix for backup files (default: cluster-id)")
	cmd.Flags().Duration("poll-interval", defaultPollInterval, "Interval between progress polls")
	cmd.Flags().Duration("rpc-timeout", defaultRPCTimeout, "Per-RPC timeout for Start, status poll, and Cancel")

	return cmd
}

func runDownload(cmd *cobra.Command, _ []string) error {
	bucketID, _ := cmd.Flags().GetString("bucket-id")
	pollInterval, _ := cmd.Flags().GetDuration("poll-interval")
	rpcTimeout, _ := cmd.Flags().GetDuration("rpc-timeout")

	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}

	if rpcTimeout <= 0 {
		rpcTimeout = defaultRPCTimeout
	}

	storage, err := cmdutil.BackupStorageFromFlags(cmd)
	if err != nil {
		return err
	}

	client, conn, err := getRestoreClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	// SIGINT/SIGTERM cancels the parent context. We treat that as "the operator
	// wants to abort": once tripped, we send a server-side CancelDownload so
	// the job stops and the staging area is cleaned up — otherwise the
	// goroutine on the server would keep running long after the CLI exits.
	rootCtx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startResp, err := startDownload(rootCtx, client, rpcTimeout, &restorepb.StartDownloadBackupRequest{
		Storage:  storage,
		BucketId: bucketID,
	})
	if err != nil {
		return cmdutil.FormatGRPCError("starting download", err)
	}

	pterm.Info.Printfln("Download started (job %s)", startResp.GetJobId())

	resp, pollErr := pollUntilTerminal(rootCtx, client, startResp.GetJobId(), pollInterval, rpcTimeout)
	if pollErr != nil {
		// A local SIGINT/SIGTERM surfaces either as context.Canceled (caught
		// between RPCs) or as a gRPC status with code Canceled (caught during
		// an in-flight RPC). Both mean "the operator wants to abort"; tell
		// the server to stop and wipe the staging dir.
		if errors.Is(pollErr, context.Canceled) || status.Code(pollErr) == codes.Canceled {
			// Use a fresh context so the cancel RPC itself is not killed by
			// the same signal that ended the poll loop.
			cancelCtx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			defer cancel()

			_, cancelErr := client.CancelDownload(cancelCtx, &restorepb.CancelDownloadRequest{JobId: startResp.GetJobId()})
			if cancelErr != nil {
				pterm.Warning.Printfln("Server-side cancel failed: %v", cancelErr)
			} else {
				pterm.Warning.Println("Download canceled")
			}

			return errors.New("download canceled")
		}

		return cmdutil.FormatGRPCError("polling download", pollErr)
	}

	switch resp.GetState() {
	case restorepb.DownloadState_DOWNLOAD_STATE_SUCCEEDED:
		pterm.Success.Printfln("Backup downloaded: %d files, %s",
			resp.GetFilesDownloaded(), cmdutil.FormatBytes(resp.GetBytesDownloaded()))

		return nil
	case restorepb.DownloadState_DOWNLOAD_STATE_CANCELED:
		pterm.Warning.Println("Download canceled by server")

		return errors.New("download canceled")
	case restorepb.DownloadState_DOWNLOAD_STATE_FAILED:
		return fmt.Errorf("download failed: %s", resp.GetErrorMessage())
	default:
		return fmt.Errorf("download ended in unexpected state %s", resp.GetState())
	}
}

// startDownload wraps StartDownloadBackup with a short per-RPC timeout, since
// the actual transfer happens asynchronously and the call itself should return
// in milliseconds.
func startDownload(
	ctx context.Context,
	client restorepb.RestoreServiceClient,
	rpcTimeout time.Duration,
	req *restorepb.StartDownloadBackupRequest,
) (*restorepb.StartDownloadBackupResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return client.StartDownloadBackup(callCtx, req)
}

// pollUntilTerminal polls GetDownloadStatus on pollInterval and renders a
// progress bar. Returns the last status response once the job reaches a
// terminal state, or an error if the parent context is canceled / a poll RPC
// fails.
func pollUntilTerminal(
	ctx context.Context,
	client restorepb.RestoreServiceClient,
	jobID string,
	pollInterval time.Duration,
	rpcTimeout time.Duration,
) (*restorepb.GetDownloadStatusResponse, error) {
	progress, _ := pterm.DefaultProgressbar.WithTitle("Downloading backup").WithTotal(1).WithShowCount(false).Start()
	defer func() { _, _ = progress.Stop() }()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		callCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
		resp, err := client.GetDownloadStatus(callCtx, &restorepb.GetDownloadStatusRequest{JobId: jobID})
		cancel()

		if err != nil {
			return nil, err
		}

		updateProgress(progress, resp)

		switch resp.GetState() {
		case restorepb.DownloadState_DOWNLOAD_STATE_SUCCEEDED,
			restorepb.DownloadState_DOWNLOAD_STATE_FAILED,
			restorepb.DownloadState_DOWNLOAD_STATE_CANCELED:
			return resp, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// updateProgress refreshes the progress bar from the last status snapshot.
// pterm's progress bar takes int totals; we scale bytes down to KiB so the
// counter stays within int range on very large (multi-TB) backups.
func updateProgress(bar *pterm.ProgressbarPrinter, resp *restorepb.GetDownloadStatusResponse) {
	const kib = 1024

	total := int(resp.GetTotalBytes() / kib)
	if total <= 0 {
		total = 1
	}

	done := min(int(resp.GetBytesDownloaded()/kib), total)

	bar.Total = total
	bar.Current = done

	title := fmt.Sprintf("Downloading: %d/%d files, %s / %s",
		resp.GetFilesDownloaded(), resp.GetTotalFiles(),
		cmdutil.FormatBytes(resp.GetBytesDownloaded()),
		cmdutil.FormatBytes(resp.GetTotalBytes()))

	if cf := resp.GetCurrentFile(); cf != "" {
		title += " (" + cf + ")"
	}

	bar.UpdateTitle(title)
}
