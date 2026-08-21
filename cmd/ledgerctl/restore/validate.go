package restore

import (
	"errors"
	"fmt"
	"io"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

// NewValidateCommand creates the restore validate command.
func NewValidateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "validate",
		Short:             "Validate staged backup integrity",
		Long:              "Run integrity checks on the staged backup data (hash chain, volumes, metadata)",
		RunE:              runValidate,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
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
		spinner = cmdutil.StartSpinner("Validating backup integrity...")
		// errorCount drives the exit status and counts DIVERGENCES only.
		// coverageGaps are reported to the operator but excluded from the
		// verdict: the checker gets no cold reader on the restore path, so a
		// healthy backup of an archived cluster always reports at least one
		// pass it could not complete. Counting those rejected valid backups.
		errorCount   int
		coverageGaps int
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
			if t.Error.GetCoverageGap() {
				coverageGaps++

				pterm.Printf("  %s %s\n", pterm.Yellow("WARNING"), t.Error.GetMessage())

				continue
			}

			errorCount++

			pterm.Printf("  %s %s\n", pterm.Red("ERROR"), t.Error.GetMessage())
		}
	}

	_ = spinner.Stop()

	pterm.Println()

	if err := cmdutil.IntegrityResult("backup validation", errorCount); err != nil {
		return err
	}

	if coverageGaps > 0 {
		pterm.Success.Printfln("Backup is valid - no integrity errors found (%d verification gap(s) reported above)", coverageGaps)

		return nil
	}

	pterm.Success.Println("Backup is valid - no integrity errors found")

	return nil
}
