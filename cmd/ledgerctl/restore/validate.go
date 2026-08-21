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
	cmd.Flags().Bool("allow-incomplete", false,
		"Exit zero when passes could not be completed, accepting a backup whose projections were never verified")

	return cmd
}

func runValidate(cmd *cobra.Command, _ []string) error {
	allowIncomplete, _ := cmd.Flags().GetBool("allow-incomplete")

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
		// errorCount counts DIVERGENCES: the backup contradicts its own audit
		// chain. coverageGaps counts passes the checker could not complete, so
		// part of the backup was never compared. The two are counted apart
		// because they mean different things and only one of them can ever be
		// accepted: the checker gets no cold reader and no baseline on the restore
		// path, so a healthy backup of an archived cluster always reports at least
		// one gap, and treating that as a divergence would reject valid backups.
		//
		// Both nonetheless fail the command. A run that skipped the projection
		// comparisons has not established that the backup is valid, and this
		// return value is the automation gate for `restore validate &&
		// restore finalize`. --allow-incomplete is how an operator accepts an
		// unverified backup; nothing accepts a divergent one.
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

	return cmdutil.ReportIntegrityVerdict(cmdutil.IntegrityVerdictInput{
		Subject:         "backup validation",
		CleanMessage:    "Backup is valid - no integrity errors found",
		Errors:          errorCount,
		CoverageGaps:    coverageGaps,
		AllowIncomplete: allowIncomplete,
	})
}
