package accounts

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/domain/analysis"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewAnalyzeCommand creates the accounts analyze command.
func NewAnalyzeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "analyze",
		Aliases: []string{"analyse"},
		Short:   "Analyze accounts and suggest account types",
		Long: `Scans all accounts in a ledger, discovers address patterns, and suggests
account types. Useful after a mirror import (v2 → v3) to understand
account structure before defining enforcement rules.

Examples:
  ledgerctl accounts analyze --ledger my-ledger
  ledgerctl accounts analyze --ledger my-ledger --threshold 20
  ledgerctl accounts analyze --ledger my-ledger --json`,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runAnalyze,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Uint32("threshold", 0, "Variable threshold (0 = default 10): max distinct children before classifying as variable")
	cmdutil.AddOutputFlags(cmd)
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

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
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

	// Suggested account types
	suggestedTypes := analysis.SuggestAccountTypes(resp)
	if len(suggestedTypes) > 0 {
		pterm.DefaultSection.Println("Suggested Account Types")
		typeTable := pterm.TableData{
			{"NAME", "PATTERN", "ENFORCEMENT"},
		}
		for _, at := range suggestedTypes {
			typeTable = append(typeTable, []string{
				at.GetName(),
				at.GetPattern(),
				"STRICT",
			})
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(typeTable).Render()
		pterm.Println()
		pterm.Println(pterm.Gray("To add these types, use:"))
		pterm.FgCyan.Println("  ledgerctl account-types add <name> <pattern> --ledger <ledger>")
	}
}
