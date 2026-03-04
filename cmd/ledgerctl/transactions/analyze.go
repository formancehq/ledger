package transactions

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
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
	cmd.Flags().Bool("json", false, "Output full response as JSON")
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
	jsonOutput, _ := cmd.Flags().GetBool("json")

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
			if err == io.EOF {
				return fmt.Errorf("stream ended without result")
			}
			return cmdutil.FormatGRPCError("failed to analyze transactions", err)
		}
		switch t := event.Type.(type) {
		case *servicepb.AnalyzeTransactionsEvent_Progress:
			p := t.Progress
			if p.Total > 0 {
				pct := p.Processed * 100 / p.Total
				spinner.UpdateText(fmt.Sprintf("Analyzing transactions... %d/%d logs (%d%%)", p.Processed, p.Total, pct))
			} else {
				spinner.UpdateText(fmt.Sprintf("Analyzing transactions... %d logs scanned", p.Processed))
			}
		case *servicepb.AnalyzeTransactionsEvent_Result:
			resp = t.Result
		}
		if resp != nil {
			break
		}
	}

	_ = spinner.Stop()

	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(resp)
	}

	renderTransactionAnalysisResult(resp)
	return nil
}

func renderTransactionAnalysisResult(resp *servicepb.AnalyzeTransactionsResponse) {
	// Summary
	pterm.DefaultHeader.WithFullWidth().Println("Transaction Flow Analysis")
	pterm.Info.Printfln("Total transactions: %d", resp.TotalTransactions)
	pterm.Info.Printfln("Total reverted: %d", resp.TotalReverted)
	pterm.Info.Printfln("Flow patterns discovered: %d", len(resp.FlowPatterns))
	pterm.Println()

	if len(resp.FlowPatterns) == 0 {
		return
	}

	// Patterns table
	pterm.DefaultSection.Println("Flow Patterns")
	tableData := pterm.TableData{
		{"SIGNATURE", "STRUCTURE", "COUNT", "ASSETS"},
	}
	for _, fp := range resp.FlowPatterns {
		var assets []string
		for _, vs := range fp.VolumeStats {
			assets = append(assets, vs.Asset)
		}
		tableData = append(tableData, []string{
			fp.Signature,
			postingStructureName(fp.Structure),
			fmt.Sprintf("%d", fp.TransactionCount),
			strings.Join(assets, ", "),
		})
	}
	_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
	pterm.Println()

	// Per-pattern details
	for _, fp := range resp.FlowPatterns {
		pterm.DefaultSection.Printfln("Pattern: %s", fp.Signature)

		if fp.Temporal != nil {
			pterm.Info.Printfln("  Period: %s - %s",
				formatTimestamp(fp.Temporal.FirstSeen),
				formatTimestamp(fp.Temporal.LastSeen))
			pterm.Info.Printfln("  Rate: %.1f tx/day", fp.Temporal.TransactionsPerDay)
		}

		if len(fp.VolumeStats) > 0 {
			volTable := pterm.TableData{
				{"ASSET", "TOTAL", "AVG", "MIN", "MAX", "COUNT"},
			}
			for _, vs := range fp.VolumeStats {
				volTable = append(volTable, []string{
					vs.Asset,
					vs.TotalVolume,
					vs.AverageVolume,
					vs.MinVolume,
					vs.MaxVolume,
					fmt.Sprintf("%d", vs.TransactionCount),
				})
			}
			_ = pterm.DefaultTable.WithHasHeader().WithData(volTable).Render()
		}

		if len(fp.MetadataKeys) > 0 {
			pterm.Info.Printfln("  Metadata keys: %s", strings.Join(fp.MetadataKeys, ", "))
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
