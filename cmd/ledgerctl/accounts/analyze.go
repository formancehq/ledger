package accounts

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewAnalyzeCommand creates the accounts analyze command.
func NewAnalyzeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "analyze",
		Aliases: []string{"analyse"},
		Short:   "Analyze accounts and suggest a Chart of Accounts",
		Long: `Scans all accounts in a ledger, discovers address patterns, and outputs
a draft Chart of Accounts. Useful after a mirror import (v2 → v3) to
understand account structure before defining enforcement rules.

Examples:
  ledgerctl accounts analyze --ledger my-ledger
  ledgerctl accounts analyze --ledger my-ledger --threshold 20
  ledgerctl accounts analyze --ledger my-ledger --json`,
		RunE: runAnalyze,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Uint32("threshold", 0, "Variable threshold (0 = default 10): max distinct children before classifying as variable")
	cmd.Flags().Bool("json", false, "Output full response as JSON")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runAnalyze(cmd *cobra.Command, _ []string) error {
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

	spinner, _ := pterm.DefaultSpinner.Start("Analyzing accounts...")

	stream, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
		Ledger:            ledgerName,
		VariableThreshold: threshold,
	})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to analyze accounts", err)
	}

	var resp *servicepb.AnalyzeAccountsResponse

	for {
		event, err := stream.Recv()
		if err != nil {
			_ = spinner.Stop()

			if errors.Is(err, io.EOF) {
				return errors.New("stream ended without result")
			}

			return cmdutil.FormatGRPCError("failed to analyze accounts", err)
		}

		switch t := event.GetType().(type) {
		case *servicepb.AnalyzeAccountsEvent_Progress:
			p := t.Progress
			spinner.UpdateText(fmt.Sprintf("Analyzing accounts... %d scanned", p.GetProcessed()))
		case *servicepb.AnalyzeAccountsEvent_Result:
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

	renderAnalysisResult(resp)

	return nil
}

func renderAnalysisResult(resp *servicepb.AnalyzeAccountsResponse) {
	// Summary
	pterm.DefaultHeader.WithFullWidth().Println("Account Analysis")
	pterm.Info.Printfln("Total accounts: %d", resp.GetTotalAccounts())
	pterm.Info.Printfln("Patterns discovered: %d", len(resp.GetPatterns()))
	pterm.Println()

	// Patterns table
	if len(resp.GetPatterns()) > 0 {
		pterm.DefaultSection.Println("Discovered Patterns")

		tableData := pterm.TableData{
			{"PATTERN", "ACCOUNTS", "ASSETS", "METADATA KEYS"},
		}
		for _, p := range resp.GetPatterns() {
			tableData = append(tableData, []string{
				p.GetPattern(),
				strconv.FormatUint(p.GetAccountCount(), 10),
				strings.Join(p.GetAssets(), ", "),
				strings.Join(p.GetMetadataKeys(), ", "),
			})
		}

		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

		pterm.Println()
	}

	// Chart tree
	if resp.GetSuggestedChart() != nil && len(resp.GetSuggestedChart().GetRoots()) > 0 {
		pterm.DefaultSection.Println("Suggested Chart of Accounts")

		tree := pterm.TreeNode{Text: "root"}

		keys := sortedMapKeys(resp.GetSuggestedChart().GetRoots())
		for _, key := range keys {
			tree.Children = append(tree.Children, buildSegmentTreeNode(key, resp.GetSuggestedChart().GetRoots()[key]))
		}

		_ = pterm.DefaultTree.WithRoot(tree).Render()
	}
}

func buildSegmentTreeNode(name string, seg *commonpb.ChartSegment) pterm.TreeNode {
	label := name
	if seg.GetAccount() {
		label += pterm.Gray(" [account]")
	}

	node := pterm.TreeNode{Text: label}

	// Add fixed children (sorted)
	for _, key := range sortedMapKeys(seg.GetChildren()) {
		node.Children = append(node.Children, buildSegmentTreeNode(key, seg.GetChildren()[key]))
	}

	// Add variable child
	if seg.GetVariable() != nil {
		node.Children = append(node.Children, buildVariableTreeNode(seg.GetVariable()))
	}

	return node
}

func buildVariableTreeNode(v *commonpb.ChartVariable) pterm.TreeNode {
	label := fmt.Sprintf("{%s}", v.GetName())
	if v.GetPattern() != "" {
		label += pterm.Gray(" ~ " + v.GetPattern())
	}

	if v.GetAccount() {
		label += pterm.Gray(" [account]")
	}

	node := pterm.TreeNode{Text: label}

	// Add fixed children
	for _, key := range sortedMapKeys(v.GetChildren()) {
		node.Children = append(node.Children, buildSegmentTreeNode(key, v.GetChildren()[key]))
	}

	// Add nested variable
	if v.GetVariable() != nil {
		node.Children = append(node.Children, buildVariableTreeNode(v.GetVariable()))
	}

	return node
}

func sortedMapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}
