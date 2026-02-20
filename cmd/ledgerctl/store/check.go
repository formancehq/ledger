package store

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewCheckCommand creates the store check command.
func NewCheckCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "check",
		Aliases: []string{"c", "verify"},
		Short:   "Check store integrity",
		Long:    "Verify hash chain integrity and derived data consistency via gRPC",
		RunE:    runCheck,
	}

	cmd.Flags().Bool("json", false, "Output as JSON instead of formatted output")
	cmd.Flags().Duration("timeout", 5*cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runCheck(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	jsonOutput, _ := cmd.Flags().GetBool("json")

	stream, err := client.CheckStore(ctx, &servicepb.CheckStoreRequest{})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to start store check", err)
	}

	var (
		spinner    *pterm.SpinnerPrinter
		errorCount int
		errors     []*servicepb.CheckStoreError
	)

	if !jsonOutput {
		spinner, _ = pterm.DefaultSpinner.Start("Checking store integrity...")
	}

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if spinner != nil {
				spinner.Fail("Failed to check store")
			}
			return cmdutil.FormatGRPCError("receiving check event", err)
		}

		switch t := event.Type.(type) {
		case *servicepb.CheckStoreEvent_Progress:
			if spinner != nil && t.Progress.TotalLogs > 0 {
				pct := float64(t.Progress.LogsChecked) / float64(t.Progress.TotalLogs) * 100
				spinner.UpdateText(fmt.Sprintf("Checking store integrity... %d/%d logs (%.0f%%)",
					t.Progress.LogsChecked, t.Progress.TotalLogs, pct))
			}

		case *servicepb.CheckStoreEvent_Error:
			errorCount++
			errors = append(errors, t.Error)
			if !jsonOutput {
				printCheckError(t.Error)
			}
		}
	}

	if spinner != nil {
		_ = spinner.Stop()
	}

	if jsonOutput {
		result := struct {
			Valid      bool                         `json:"valid"`
			ErrorCount int                          `json:"errorCount"`
			Errors     []*servicepb.CheckStoreError `json:"errors,omitempty"`
		}{
			Valid:      errorCount == 0,
			ErrorCount: errorCount,
			Errors:     errors,
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}

	pterm.Println()
	if errorCount == 0 {
		pterm.Success.Println("Store is valid - no integrity errors found")
	} else {
		pterm.Error.Printfln("%d integrity error(s) found", errorCount)
	}

	return nil
}

func printCheckError(e *servicepb.CheckStoreError) {
	prefix := pterm.Red("ERROR")
	errorTypeName := strings.TrimPrefix(e.ErrorType.String(), "CHECK_STORE_ERROR_TYPE_")
	details := fmt.Sprintf("[%s]", errorTypeName)

	if e.LogSequence > 0 {
		details += fmt.Sprintf(" log=%d", e.LogSequence)
	}
	if e.Ledger != "" {
		details += fmt.Sprintf(" ledger=%s", e.Ledger)
	}
	if e.Account != "" {
		details += fmt.Sprintf(" account=%s", e.Account)
	}
	if e.Asset != "" {
		details += fmt.Sprintf(" asset=%s", e.Asset)
	}
	if e.TransactionId > 0 {
		details += fmt.Sprintf(" tx=%d", e.TransactionId)
	}

	pterm.Printf("  %s %s: %s\n", prefix, pterm.Gray(details), e.Message)
}
