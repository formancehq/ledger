package store

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/application/check"
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
	cmd.Flags().Bool("allow-incomplete", false,
		"Exit zero when passes could not be completed, accepting a store whose projections were never verified")

	return cmd
}

// checkFindings separates the two kinds of finding one check can report, as the
// event stream arrives.
//
// A divergence says the store contradicts its own audit chain. A coverage gap
// says a pass could not be completed, so part of the store was never compared —
// nothing about it is known to be wrong. CheckStoreEvent carries only Progress
// and Error variants, so both ride the error channel and the consumer is what
// tells them apart (check.IsCoverageGap). Counting a gap as a divergence is the
// misclassification cmdutil.ReportIntegrityVerdict exists to prevent, and it is
// reachable here: `store check` runs against a live node, which has a cold
// reader but not necessarily a baseline checkpoint — a node restored from a
// backup holds archived chapters and no baseline until it closes a chapter of
// its own, and createBaselineSnapshot is non-fatal on error, so a single failed
// snapshot leaves the same shape until the following close.
//
// Kept apart from runCheck so the classification and the payload it feeds are
// testable without a gRPC stream or a terminal, the same reason
// cmdutil.ClassifyIntegrity is split out of ReportIntegrityVerdict.
type checkFindings struct {
	divergences  []*servicepb.CheckStoreError
	coverageGaps []*servicepb.CheckStoreError
}

// add files one finding and reports whether it was a coverage gap rather than a
// divergence, so the caller can label the line it prints.
func (f *checkFindings) add(e *servicepb.CheckStoreError) bool {
	if check.IsCoverageGap(e.GetErrorType()) {
		f.coverageGaps = append(f.coverageGaps, e)

		return true
	}

	f.divergences = append(f.divergences, e)

	return false
}

// checkResult is the machine-readable verdict of one store check.
type checkResult struct {
	// Valid is reserved for the clean outcome: every pass ran and none found a
	// divergence. An incomplete run is not valid, and it is not an error either.
	Valid bool `json:"valid"`
	// Outcome names the three-way verdict, so a consumer need not reimplement
	// the precedence rule (a divergence outranks any number of gaps) from the
	// counts. Same reason restorepb carries CoverageGap rather than the error
	// type: the classification is the contract, not its inputs.
	Outcome string `json:"outcome"`
	// ErrorCount counts divergences only.
	ErrorCount int `json:"errorCount"`
	// CoverageGapCount counts passes that could not be completed.
	CoverageGapCount int `json:"coverageGapCount"`
	// Errors holds the divergences, CoverageGaps the incomplete passes. Split so
	// a structured consumer can act on the two differently, which a single list
	// plus a bare `valid: false` never allowed.
	Errors       []*servicepb.CheckStoreError `json:"errors,omitempty"`
	CoverageGaps []*servicepb.CheckStoreError `json:"coverageGaps,omitempty"`
}

// result projects the findings onto the structured payload.
func (f *checkFindings) result() checkResult {
	outcome := cmdutil.ClassifyIntegrity(len(f.divergences), len(f.coverageGaps))

	return checkResult{
		Valid:            outcome == cmdutil.IntegrityOutcomeClean,
		Outcome:          outcome.String(),
		ErrorCount:       len(f.divergences),
		CoverageGapCount: len(f.coverageGaps),
		Errors:           f.divergences,
		CoverageGaps:     f.coverageGaps,
	}
}

func runCheck(cmd *cobra.Command, _ []string) error {
	allowIncomplete, _ := cmd.Flags().GetBool("allow-incomplete")

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
		spinner  *cmdutil.Spinner
		findings checkFindings
	)

	if !structuredOutput {
		spinner = cmdutil.StartSpinner("Checking store integrity...")
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
			coverageGap := findings.add(t.Error)
			if !structuredOutput {
				printCheckFinding(t.Error, coverageGap)
			}
		}
	}

	if spinner != nil {
		_ = spinner.Stop()
	}

	if handled, err := cmdutil.EncodeStructured(cmd, findings.result()); handled || err != nil {
		return err
	}

	pterm.Println()

	return cmdutil.ReportIntegrityVerdict(cmdutil.IntegrityVerdictInput{
		Subject:         "store validation",
		CleanMessage:    "Store is valid - no integrity errors found",
		Errors:          len(findings.divergences),
		CoverageGaps:    len(findings.coverageGaps),
		AllowIncomplete: allowIncomplete,
	})
}

// printCheckFinding renders one finding. coverageGap picks the label: a pass that
// could not be completed is a WARNING, not an ERROR, because it says nothing
// about the store being wrong.
func printCheckFinding(e *servicepb.CheckStoreError, coverageGap bool) {
	prefix := pterm.Red("ERROR")
	if coverageGap {
		prefix = pterm.Yellow("WARNING")
	}

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
