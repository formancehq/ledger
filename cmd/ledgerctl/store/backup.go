package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// NewBackupCommand creates the store backup command.
func NewBackupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "backup",
		Aliases: []string{"bk"},
		Short:   "Download a point-in-time backup",
		Long:    "Stream a tar archive backup of the Pebble store from the cluster leader",
		RunE:    runBackup,
	}

	cmd.Flags().StringP("output", "o", "", "Output file path (required if stdout is a terminal)")
	cmd.Flags().Duration("timeout", 10*cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runBackup(cmd *cobra.Command, _ []string) error {
	outputPath, _ := cmd.Flags().GetString("output")

	// Refuse to write binary to terminal
	if outputPath == "" && term.IsTerminal(int(os.Stdout.Fd())) {
		return fmt.Errorf("refusing to write binary data to terminal; use --output/-o to specify a file")
	}

	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	stream, err := client.Backup(ctx, &clusterpb.BackupRequest{})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to start backup", err)
	}

	// Open output
	var out *os.File
	if outputPath != "" {
		out, err = os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("creating output file: %w", err)
		}
		defer func() { _ = out.Close() }()
	} else {
		out = os.Stdout
	}

	var (
		spinner       *pterm.SpinnerPrinter
		progressBar   *pterm.ProgressbarPrinter
		totalReceived uint64
		estimatedSize uint64
		hash          = sha256.New()
		expectedHash  string
		expectedSize  uint64
		interactive   = outputPath != ""
	)

	if interactive {
		spinner, _ = pterm.DefaultSpinner.Start("Preparing backup...")
	}

	stopProgress := func() {
		if progressBar != nil {
			_, _ = progressBar.Stop()
		}
		if spinner != nil {
			_ = spinner.Stop()
		}
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			stopProgress()
			return cmdutil.FormatGRPCError("receiving backup chunk", err)
		}

		// Status-only messages update the spinner during preparation phases
		if resp.StatusMessage != "" && len(resp.Data) == 0 && !resp.Eof {
			if spinner != nil {
				spinner.UpdateText(resp.StatusMessage)
			}
			continue
		}

		if resp.Eof {
			expectedHash = resp.ContentSha256
			expectedSize = resp.ContentSize
			break
		}

		if len(resp.Data) > 0 {
			if _, err := out.Write(resp.Data); err != nil {
				stopProgress()
				pterm.Error.Printfln("writing backup data: %v", err)
				return cmdutil.Displayed(fmt.Errorf("writing backup data: %w", err))
			}
			if _, err := hash.Write(resp.Data); err != nil {
				stopProgress()
				pterm.Error.Printfln("computing hash: %v", err)
				return cmdutil.Displayed(fmt.Errorf("computing hash: %w", err))
			}
			totalReceived += uint64(len(resp.Data))

			if resp.EstimatedTotalSize > 0 {
				estimatedSize = resp.EstimatedTotalSize
			}

			if !interactive {
				continue
			}

			// Switch from spinner to progress bar once we know the estimated size
			if progressBar == nil && estimatedSize > 0 {
				if spinner != nil {
					_ = spinner.Stop()
					spinner = nil
				}
				progressBar, _ = pterm.DefaultProgressbar.
					WithTotal(int(estimatedSize)).
					WithShowCount(false).
					WithShowPercentage(true).
					WithShowElapsedTime(true).
					WithTitle(fmt.Sprintf("Downloading backup... %s / %s",
						cmdutil.FormatBytes(totalReceived), cmdutil.FormatBytes(estimatedSize))).
					Start()
				progressBar.Add(int(totalReceived))
			} else if progressBar != nil {
				progressBar.Add(len(resp.Data))
				progressBar.UpdateTitle(fmt.Sprintf("Downloading backup... %s / %s",
					cmdutil.FormatBytes(totalReceived), cmdutil.FormatBytes(estimatedSize)))
			} else if spinner != nil {
				spinner.UpdateText(fmt.Sprintf("Downloading backup... %s",
					cmdutil.FormatBytes(totalReceived)))
			}
		}
	}

	if progressBar != nil {
		_, _ = progressBar.Stop()
	}
	if spinner != nil {
		_ = spinner.Stop()
	}

	// Verify hash
	actualHash := hex.EncodeToString(hash.Sum(nil))
	if expectedHash != "" && actualHash != expectedHash {
		if interactive {
			pterm.Error.Printfln("SHA256 mismatch: expected %s, got %s", expectedHash, actualHash)
		}
		return cmdutil.Displayed(fmt.Errorf("backup integrity check failed: SHA256 mismatch"))
	}

	if interactive {
		pterm.Success.Printfln("Backup saved to %s (%s, SHA256: %s)",
			pterm.Cyan(outputPath), cmdutil.FormatBytes(expectedSize), actualHash[:12]+"...")
	}

	return nil
}
