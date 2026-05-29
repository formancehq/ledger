package store

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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

	cmdutil.AddOutputFlags(cmd)
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

	structuredOutput := cmdutil.IsStructuredOutput(cmd)

	stream, err := client.CheckStore(ctx, &servicepb.CheckStoreRequest{})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to start store check", err)
	}

	var (
		spinner     *pterm.SpinnerPrinter
		errorCount  int
		checkErrors []*servicepb.CheckStoreError
	)

	if !structuredOutput {
		spinner, _ = pterm.DefaultSpinner.Start("Checking store integrity...")
	}

	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			if spinner != nil {
				_ = spinner.Stop()
			}

			return cmdutil.FormatGRPCError("receiving check event", err)
		}

		switch t := event.GetType().(type) {
		case *servicepb.CheckStoreEvent_Progress:
			if spinner != nil && t.Progress.GetTotalLogs() > 0 {
				pct := float64(t.Progress.GetLogsChecked()) / float64(t.Progress.GetTotalLogs()) * 100
				spinner.UpdateText(fmt.Sprintf("Checking store integrity... %d/%d logs (%.0f%%)",
					t.Progress.GetLogsChecked(), t.Progress.GetTotalLogs(), pct))
			}

		case *servicepb.CheckStoreEvent_Error:
			errorCount++

			checkErrors = append(checkErrors, t.Error)
			if !structuredOutput {
				printCheckError(t.Error)
			}
		}
	}

	if spinner != nil {
		_ = spinner.Stop()
	}

	if handled, err := cmdutil.EncodeStructured(cmd, struct {
		Valid      bool                         `json:"valid"`
		ErrorCount int                          `json:"errorCount"`
		Errors     []*servicepb.CheckStoreError `json:"errors,omitempty"`
	}{
		Valid:      errorCount == 0,
		ErrorCount: errorCount,
		Errors:     checkErrors,
	}); handled || err != nil {
		return err
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
	errorTypeName := strings.TrimPrefix(e.GetErrorType().String(), "CHECK_STORE_ERROR_TYPE_")
	details := fmt.Sprintf("[%s]", errorTypeName)

	if e.GetLogSequence() > 0 {
		details += fmt.Sprintf(" log=%d", e.GetLogSequence())
	}

	if e.GetLedger() != "" {
		details += " ledger=" + e.GetLedger()
	}

	if e.GetAccount() != "" {
		details += " account=" + e.GetAccount()
	}

	if e.GetAsset() != "" {
		details += " asset=" + e.GetAsset()
	}

	if e.GetTransactionId() > 0 {
		details += fmt.Sprintf(" tx=%d", e.GetTransactionId())
	}

	pterm.Printf("  %s %s: %s\n", prefix, pterm.Gray(details), e.GetMessage())
}
