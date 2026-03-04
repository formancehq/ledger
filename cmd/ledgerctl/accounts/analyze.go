package accounts

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
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

	resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
		Ledger:            ledgerName,
		VariableThreshold: threshold,
	})
	if err != nil {
		_ = spinner.Stop()
		return cmdutil.FormatGRPCError("failed to analyze accounts", err)
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
	pterm.Info.Printfln("Total accounts: %d", resp.TotalAccounts)
	pterm.Info.Printfln("Patterns discovered: %d", len(resp.Patterns))
	pterm.Println()

	// Patterns table
	if len(resp.Patterns) > 0 {
		pterm.DefaultSection.Println("Discovered Patterns")
		tableData := pterm.TableData{
			{"PATTERN", "ACCOUNTS", "ASSETS", "METADATA KEYS"},
		}
		for _, p := range resp.Patterns {
			tableData = append(tableData, []string{
				p.Pattern,
				fmt.Sprintf("%d", p.AccountCount),
				strings.Join(p.Assets, ", "),
				strings.Join(p.MetadataKeys, ", "),
			})
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
		pterm.Println()
	}

	// Chart tree
	if resp.SuggestedChart != nil && len(resp.SuggestedChart.Roots) > 0 {
		pterm.DefaultSection.Println("Suggested Chart of Accounts")
		tree := pterm.TreeNode{Text: "root"}
		keys := sortedMapKeys(resp.SuggestedChart.Roots)
		for _, key := range keys {
			tree.Children = append(tree.Children, buildSegmentTreeNode(key, resp.SuggestedChart.Roots[key]))
		}
		_ = pterm.DefaultTree.WithRoot(tree).Render()
	}
}

func buildSegmentTreeNode(name string, seg *commonpb.ChartSegment) pterm.TreeNode {
	label := name
	if seg.Account {
		label += pterm.Gray(" [account]")
	}

	node := pterm.TreeNode{Text: label}

	// Add fixed children (sorted)
	for _, key := range sortedMapKeys(seg.Children) {
		node.Children = append(node.Children, buildSegmentTreeNode(key, seg.Children[key]))
	}

	// Add variable child
	if seg.Variable != nil {
		node.Children = append(node.Children, buildVariableTreeNode(seg.Variable))
	}

	return node
}

func buildVariableTreeNode(v *commonpb.ChartVariable) pterm.TreeNode {
	label := fmt.Sprintf("{%s}", v.Name)
	if v.Pattern != "" {
		label += pterm.Gray(fmt.Sprintf(" ~ %s", v.Pattern))
	}
	if v.Account {
		label += pterm.Gray(" [account]")
	}

	node := pterm.TreeNode{Text: label}

	// Add fixed children
	for _, key := range sortedMapKeys(v.Children) {
		node.Children = append(node.Children, buildSegmentTreeNode(key, v.Children[key]))
	}

	// Add nested variable
	if v.Variable != nil {
		node.Children = append(node.Children, buildVariableTreeNode(v.Variable))
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
