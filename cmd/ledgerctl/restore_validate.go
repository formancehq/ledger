package main

import (
	"fmt"
	"io"

	"github.com/formancehq/ledger-v3-poc/internal/proto/restorepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newRestoreValidateCommand creates the restore validate command.
func newRestoreValidateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate staged backup integrity",
		Long:  "Run integrity checks on the staged backup data (hash chain, volumes, metadata)",
		RunE:  runRestoreValidate,
	}

	cmd.Flags().Duration("timeout", 5*defaultTimeout, "Request timeout")

	return cmd
}

func runRestoreValidate(cmd *cobra.Command, _ []string) error {
	client, conn, err := getRestoreClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	stream, err := client.ValidateRestore(ctx, &restorepb.ValidateRestoreRequest{})
	if err != nil {
		return formatGRPCError("failed to start validation", err)
	}

	var (
		spinner, _ = pterm.DefaultSpinner.Start("Validating backup integrity...")
		errorCount int
	)

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			spinner.Fail("Validation failed")
			return formatGRPCError("receiving validation event", err)
		}

		switch t := event.Type.(type) {
		case *restorepb.ValidateRestoreEvent_Progress:
			if t.Progress.TotalLogs > 0 {
				pct := float64(t.Progress.LogsChecked) / float64(t.Progress.TotalLogs) * 100
				spinner.UpdateText(fmt.Sprintf("Validating backup integrity... %d/%d logs (%.0f%%)",
					t.Progress.LogsChecked, t.Progress.TotalLogs, pct))
			}
		case *restorepb.ValidateRestoreEvent_Error:
			errorCount++
			pterm.Printf("  %s %s\n", pterm.Red("ERROR"), t.Error.Message)
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
