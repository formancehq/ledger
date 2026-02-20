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
		totalReceived uint64
		hash          = sha256.New()
		expectedHash  string
		expectedSize  uint64
	)

	if outputPath != "" {
		spinner, _ = pterm.DefaultSpinner.Start("Downloading backup...")
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if spinner != nil {
				spinner.Fail("Backup failed")
			}
			return cmdutil.FormatGRPCError("receiving backup chunk", err)
		}

		if resp.Eof {
			expectedHash = resp.ContentSha256
			expectedSize = resp.ContentSize
			break
		}

		if len(resp.Data) > 0 {
			if _, err := out.Write(resp.Data); err != nil {
				if spinner != nil {
					spinner.Fail("Backup failed")
				}
				return fmt.Errorf("writing backup data: %w", err)
			}
			if _, err := hash.Write(resp.Data); err != nil {
				if spinner != nil {
					spinner.Fail("Backup failed")
				}
				return fmt.Errorf("computing hash: %w", err)
			}
			totalReceived += uint64(len(resp.Data))

			if spinner != nil {
				spinner.UpdateText(fmt.Sprintf("Downloading backup... %s", cmdutil.FormatBytes(totalReceived)))
			}
		}
	}

	if spinner != nil {
		_ = spinner.Stop()
	}

	// Verify hash
	actualHash := hex.EncodeToString(hash.Sum(nil))
	if expectedHash != "" && actualHash != expectedHash {
		if outputPath != "" {
			pterm.Error.Printfln("SHA256 mismatch: expected %s, got %s", expectedHash, actualHash)
		}
		return fmt.Errorf("backup integrity check failed: SHA256 mismatch")
	}

	if outputPath != "" {
		pterm.Success.Printfln("Backup saved to %s (%s, SHA256: %s)",
			pterm.Cyan(outputPath), cmdutil.FormatBytes(expectedSize), actualHash[:12]+"...")
	}

	return nil
}
