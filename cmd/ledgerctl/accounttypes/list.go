package accounttypes

import (
	"fmt"
	"sort"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// NewListCommand creates the account-types list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: cmdutil.ListAliases,
		Short:   "List all account types for a ledger",
		Long: `List all account types configured on a ledger.

Account types are embedded in the ledger configuration and naturally bounded
in size; this endpoint is intentionally not paginated.

If --ledger is not provided and only one ledger exists, it will be used automatically.

Examples:
  ledgerctl account-types list --ledger my-ledger
  ledgerctl at ls`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runList,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmdutil.AddOutputFlags(cmd)
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

	if handled, err := cmdutil.EncodeStructured(cmd, info.GetAccountTypes()); handled || err != nil {
		return err
	}

	if len(info.GetAccountTypes()) == 0 {
		pterm.Info.Printfln("No account types configured on ledger %s.", ledgerName)
		pterm.Println(pterm.Gray("Hint: Add an account type using:"))
		pterm.FgCyan.Printfln("  ledgerctl account-types add <name> <pattern> --ledger %s", ledgerName)

		return nil
	}

	// Sort by name for consistent output.
	names := make([]string, 0, len(info.GetAccountTypes()))
	for n := range info.GetAccountTypes() {
		names = append(names, n)
	}
	sort.Strings(names)

	tableData := pterm.TableData{
		{"NAME", "PATTERN", "PERSISTENCE"},
	}

	for _, n := range names {
		at := info.GetAccountTypes()[n]
		tableData = append(tableData, []string{
			at.GetName(),
			at.GetPattern(),
			FormatPersistence(at.GetPersistence()),
		})
	}

	pterm.Printf("Ledger: %s (%d account types)\n\n", pterm.Cyan(ledgerName), len(names))

	return pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}

// FormatPersistence returns the canonical uppercase name for a persistence
// enum. It delegates to the shared commonpb helper so the CLI, HTTP API, and
// OpenAPI spec stay in sync.
func FormatPersistence(p commonpb.AccountTypePersistence) string {
	return commonpb.PersistenceToString(p)
}

// ParsePersistence converts a persistence string to its enum. It delegates to
// the shared commonpb helper (case-insensitive, empty defaults to NORMAL).
func ParsePersistence(s string) (commonpb.AccountTypePersistence, error) {
	return commonpb.ParsePersistence(s)
}
