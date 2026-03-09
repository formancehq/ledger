package transactions

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewAnalyzeCommand creates the transactions analyze command.
func NewAnalyzeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "analyze",
		Aliases: []string{"analyse"},
		Short:   "Analyze transaction flow patterns",
		Long: `Scans all transactions in a ledger, discovers flow patterns by normalizing
account addresses, and outputs statistics per flow type. Useful for
understanding transaction structure and volume distribution.

Examples:
  ledgerctl transactions analyze --ledger my-ledger
  ledgerctl transactions analyze --ledger my-ledger --threshold 20
  ledgerctl transactions analyze --ledger my-ledger --json`,
		RunE: runAnalyzeTransactions,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Uint32("threshold", 0, "Variable threshold (0 = default 10): max distinct children before classifying as variable")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runAnalyzeTransactions(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ledgerFlag, _ := cmd.Flags().GetString("ledger")

	ledgerName, err := cmdutil.SelectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

	threshold, _ := cmd.Flags().GetUint32("threshold")

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Analyzing transactions...")

	stream, err := client.AnalyzeTransactions(ctx, &servicepb.AnalyzeTransactionsRequest{
		Ledger:            ledgerName,
		VariableThreshold: threshold,
	})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to analyze transactions", err)
	}

	var resp *servicepb.AnalyzeTransactionsResponse

	for {
		event, err := stream.Recv()
		if err != nil {
			_ = spinner.Stop()

			if errors.Is(err, io.EOF) {
				return errors.New("stream ended without result")
			}

			return cmdutil.FormatGRPCError("failed to analyze transactions", err)
		}

		switch t := event.GetType().(type) {
		case *servicepb.AnalyzeTransactionsEvent_Progress:
			p := t.Progress
			if p.GetTotal() > 0 {
				pct := p.GetProcessed() * 100 / p.GetTotal()
				spinner.UpdateText(fmt.Sprintf("Analyzing transactions... %d/%d logs (%d%%)", p.GetProcessed(), p.GetTotal(), pct))
			} else {
				spinner.UpdateText(fmt.Sprintf("Analyzing transactions... %d logs scanned", p.GetProcessed()))
			}
		case *servicepb.AnalyzeTransactionsEvent_Result:
			resp = t.Result
		}

		if resp != nil {
			break
		}
	}

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	renderTransactionAnalysisResult(resp)

	return nil
}

func renderTransactionAnalysisResult(resp *servicepb.AnalyzeTransactionsResponse) {
	// Summary
	pterm.DefaultHeader.WithFullWidth().Println("Transaction Flow Analysis")
	pterm.Info.Printfln("Total transactions: %d", resp.GetTotalTransactions())
	pterm.Info.Printfln("Total reverted: %d", resp.GetTotalReverted())
	pterm.Info.Printfln("Flow patterns discovered: %d", len(resp.GetFlowPatterns()))
	pterm.Println()

	if len(resp.GetFlowPatterns()) == 0 {
		return
	}

	// Patterns table
	pterm.DefaultSection.Println("Flow Patterns")

	tableData := pterm.TableData{
		{"#", "STRUCTURE", "COUNT", "ASSETS"},
	}

	for i, fp := range resp.GetFlowPatterns() {
		var assets []string
		for _, vs := range fp.GetVolumeStats() {
			assets = append(assets, vs.GetAsset())
		}

		tableData = append(tableData, []string{
			strconv.Itoa(i + 1),
			postingStructureName(fp.GetStructure()),
			strconv.FormatUint(fp.GetTransactionCount(), 10),
			strings.Join(assets, ", "),
		})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

	pterm.Println()

	// Per-pattern details
	for i, fp := range resp.GetFlowPatterns() {
		pterm.DefaultSection.Printfln("Pattern #%d (%s, %d tx)", i+1, postingStructureName(fp.GetStructure()), fp.GetTransactionCount())

		for _, p := range fp.GetPostings() {
			pterm.Printfln("  %s -> %s [%s]", p.GetSourcePattern(), p.GetDestinationPattern(), p.GetAsset())
		}

		if fp.GetTemporal() != nil {
			pterm.Println()
			pterm.Info.Printfln("  Period: %s - %s",
				formatTimestamp(fp.GetTemporal().GetFirstSeen()),
				formatTimestamp(fp.GetTemporal().GetLastSeen()))
			pterm.Info.Printfln("  Rate: %.1f tx/day", fp.GetTemporal().GetTransactionsPerDay())
		}

		if len(fp.GetVolumeStats()) > 0 {
			pterm.Println()

			volTable := pterm.TableData{
				{"ASSET", "TOTAL", "AVG", "MIN", "MAX", "COUNT"},
			}
			for _, vs := range fp.GetVolumeStats() {
				volTable = append(volTable, []string{
					vs.GetAsset(),
					vs.GetTotalVolume(),
					vs.GetAverageVolume(),
					vs.GetMinVolume(),
					vs.GetMaxVolume(),
					strconv.FormatUint(vs.GetTransactionCount(), 10),
				})
			}

			_ = pterm.DefaultTable.WithHasHeader().WithData(volTable).Render()
		}

		if len(fp.GetMetadataKeys()) > 0 {
			pterm.Info.Printfln("  Metadata keys: %s", strings.Join(fp.GetMetadataKeys(), ", "))
		}

		pterm.Println()
	}
}

func postingStructureName(s servicepb.PostingStructure) string {
	switch s {
	case servicepb.PostingStructure_POSTING_STRUCTURE_SIMPLE:
		return "simple"
	case servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_SOURCE:
		return "multi-source"
	case servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_DESTINATION:
		return "multi-destination"
	case servicepb.PostingStructure_POSTING_STRUCTURE_COMPLEX:
		return "complex"
	default:
		return "unknown"
	}
}

func formatTimestamp(ts *commonpb.Timestamp) string {
	if ts == nil {
		return "N/A"
	}

	return ts.AsTime().Format("2006-01-02 15:04:05 UTC")
}
