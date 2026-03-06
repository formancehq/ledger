package restore

import (
	"errors"
	"fmt"
	"io"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/restorepb"
)

// NewValidateCommand creates the restore validate command.
func NewValidateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate staged backup integrity",
		Long:  "Run integrity checks on the staged backup data (hash chain, volumes, metadata)",
		RunE:  runValidate,
	}

	cmd.Flags().Duration("timeout", 5*cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runValidate(cmd *cobra.Command, _ []string) error {
	client, conn, err := getRestoreClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	stream, err := client.ValidateRestore(ctx, &restorepb.ValidateRestoreRequest{})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to start validation", err)
	}

	var (
		spinner, _ = pterm.DefaultSpinner.Start("Validating backup integrity...")
		errorCount int
	)

	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			_ = spinner.Stop()

			return cmdutil.FormatGRPCError("receiving validation event", err)
		}

		switch t := event.GetType().(type) {
		case *restorepb.ValidateRestoreEvent_Progress:
			if t.Progress.GetTotalLogs() > 0 {
				pct := float64(t.Progress.GetLogsChecked()) / float64(t.Progress.GetTotalLogs()) * 100
				spinner.UpdateText(fmt.Sprintf("Validating backup integrity... %d/%d logs (%.0f%%)",
					t.Progress.GetLogsChecked(), t.Progress.GetTotalLogs(), pct))
			}
		case *restorepb.ValidateRestoreEvent_Error:
			errorCount++

			pterm.Printf("  %s %s\n", pterm.Red("ERROR"), t.Error.GetMessage())
		}
	}

	_ = spinner.Stop()

	pterm.Println()

	if errorCount == 0 {
		pterm.Success.Println("Backup is valid - no integrity errors found")
	} else {
		pterm.Error.Printfln("%d integrity error(s) found", errorCount)
	}

	return nil
}
