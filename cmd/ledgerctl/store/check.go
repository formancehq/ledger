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
		Use:               "check",
		Aliases:           []string{"c", "verify"},
		Short:             "Check store integrity",
		Long:              "Verify hash chain integrity and derived data consistency via gRPC",
		RunE:              runCheck,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
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
		spinner            *pterm.SpinnerPrinter
		errorCount         int
		checkErrors        []*servicepb.CheckStoreError
		unverifiableRanges []*servicepb.CheckStoreUnverifiableRange
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

		case *servicepb.CheckStoreEvent_UnverifiableRange:
			// NOT a divergence, so it must not touch errorCount or the exit
			// code: "cannot prove" is not "diverges", and the malformed-entry
			// case already emits its own AUDIT_STRUCTURE_INVALID (EN-1526).
			unverifiableRanges = append(unverifiableRanges, t.UnverifiableRange)

			if !structuredOutput {
				printCheckUnverifiable(t.UnverifiableRange)
			}
		}
	}

	if spinner != nil {
		_ = spinner.Stop()
	}

	if handled, err := cmdutil.EncodeStructured(cmd, struct {
		Valid              bool                                     `json:"valid"`
		ErrorCount         int                                      `json:"errorCount"`
		Errors             []*servicepb.CheckStoreError             `json:"errors,omitempty"`
		UnverifiableRanges []*servicepb.CheckStoreUnverifiableRange `json:"unverifiableRanges,omitempty"`
	}{
		Valid:              errorCount == 0,
		ErrorCount:         errorCount,
		Errors:             checkErrors,
		UnverifiableRanges: unverifiableRanges,
	}); handled || err != nil {
		return err
	}

	pterm.Println()

	// Printed before IntegrityResult, which returns a non-nil error as soon as
	// errorCount > 0: a store with both divergences and unverifiable ranges is
	// exactly where the caveat matters most, and placing this after the gate
	// would drop it on that path.
	if len(unverifiableRanges) > 0 {
		pterm.Warning.Printfln("%d range(s) could not be authenticated; absence of errors there is not proof", len(unverifiableRanges))
	}

	if err := cmdutil.IntegrityResult("store validation", errorCount); err != nil {
		return err
	}

	pterm.Success.Println("Store is valid - no integrity errors found")

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

// printCheckUnverifiable renders a range the checker could not authenticate.
// It is a warning, never an error: the absence of a finding in an
// unauthenticated range is not proof of integrity. Every occurrence still
// marks a defect, so it is surfaced rather than hidden.
func printCheckUnverifiable(u *servicepb.CheckStoreUnverifiableRange) {
	prefix := pterm.Yellow("WARN")

	// Mirror printCheckError's "[TYPE]" shape using the typed reason, so the
	// output is greppable without parsing prose.
	reasonName := strings.TrimPrefix(u.GetReason().String(), "CHECK_STORE_UNVERIFIABLE_REASON_")
	details := fmt.Sprintf("[UNVERIFIABLE %s]", reasonName)

	// Log sequences are 1-based, so a zero or inverted range means the bounds
	// could not be determined. Never render that as a literal "0-0".
	start, end := u.GetRangeStart(), u.GetRangeEnd()
	if start > 0 && end >= start {
		details += fmt.Sprintf(" logs=%d-%d", start, end)
	} else {
		details += " logs=undetermined"
	}

	pterm.Printfln("%s %s %s", prefix, details, u.GetMessage())
}
