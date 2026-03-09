package accounttypes

import (
	"fmt"
	"sort"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewListCommand creates the account-types list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all account types for a ledger",
		Long: `List all account types configured on a ledger.

If --ledger is not provided and only one ledger exists, it will be used automatically.

Examples:
  ledgerctl account-types list --ledger my-ledger
  ledgerctl at ls`,
		Args: cobra.NoArgs,
		RunE: runList,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runList(cmd *cobra.Command, _ []string) error {
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

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	ledgers, err := cmdutil.GetAllLedgersInfo(ctx, client)
	if err != nil {
		return cmdutil.FormatGRPCError("failed to list ledgers", err)
	}

	info, ok := ledgers[ledgerName]
	if !ok {
		return fmt.Errorf("ledger %q not found", ledgerName)
	}

	if len(info.AccountTypes) == 0 {
		pterm.Info.Printfln("No account types configured on ledger %s.", ledgerName)
		pterm.Println(pterm.Gray("Hint: Add an account type using:"))
		pterm.FgCyan.Printfln("  ledgerctl account-types add <name> <pattern> --ledger %s", ledgerName)
		return nil
	}

	// Sort by name for consistent output.
	names := make([]string, 0, len(info.AccountTypes))
	for n := range info.AccountTypes {
		names = append(names, n)
	}
	sort.Strings(names)

	tableData := pterm.TableData{
		{"NAME", "PATTERN", "STATUS", "ENFORCEMENT", "SUPERSEDED BY"},
	}

	for _, n := range names {
		at := info.AccountTypes[n]
		tableData = append(tableData, []string{
			at.Name,
			at.Pattern,
			formatStatus(at.Status),
			formatEnforcementMode(at.EnforcementMode),
			at.SupersededBy,
		})
	}

	pterm.Printf("Ledger: %s (%d account types)\n\n", pterm.Cyan(ledgerName), len(names))

	return pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}

func formatStatus(s commonpb.AccountTypeStatus) string {
	switch s {
	case commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE:
		return "ACTIVE"
	case commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING:
		return "MIGRATING"
	case commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED:
		return "DEPRECATED"
	default:
		return "UNKNOWN"
	}
}

func formatEnforcementMode(m commonpb.ChartEnforcementMode) string {
	switch m {
	case commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT:
		return "STRICT"
	case commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT:
		return "AUDIT"
	default:
		return "UNKNOWN"
	}
}
