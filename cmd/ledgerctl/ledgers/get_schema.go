package ledgers

import (
	"fmt"
	"sort"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewGetSchemaCommand creates the ledgers get-schema command.
func NewGetSchemaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get-schema <name>",
		Aliases: []string{"schema", "gs"},
		Short:   "Get metadata schema for a ledger",
		Long: `Display the declared metadata schema for a ledger.

Shows tables (Account Fields, Transaction Fields, Ledger Fields) with columns: KEY, TYPE.
The type is the declared type the indexer encodes forward entries under;
reads return stored metadata verbatim regardless of the declaration.

Examples:
  ledgerctl ledgers get-schema my-ledger
  ledgerctl ledgers schema my-ledger
  ledgerctl ledgers gs my-ledger --json`,
		Args:              cobra.ExactArgs(1),
		RunE:              runGetSchema,
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runGetSchema(cmd *cobra.Command, args []string) error {
	ledgerName := args[0]

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching schema for %s...", ledgerName))

	resp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
		Ledger: ledgerName,
	})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get schema status", err)
	}

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	pterm.Println()
	pterm.Printf("Metadata Schema: %s\n", ledgerName)
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	hasAccountFields := len(resp.GetAccountFields()) > 0
	hasTransactionFields := len(resp.GetTransactionFields()) > 0
	hasLedgerFields := len(resp.GetLedgerFields()) > 0

	if !hasAccountFields && !hasTransactionFields && !hasLedgerFields {
		pterm.Println(pterm.Gray("(no schema defined)"))

		return nil
	}

	if hasAccountFields {
		pterm.Println("Account Fields:")
		renderSchemaStatusTable(resp.GetAccountFields())
		pterm.Println()
	}

	if hasTransactionFields {
		pterm.Println("Transaction Fields:")
		renderSchemaStatusTable(resp.GetTransactionFields())
		pterm.Println()
	}

	if hasLedgerFields {
		pterm.Println("Ledger Fields:")
		renderSchemaStatusTable(resp.GetLedgerFields())
		pterm.Println()
	}

	return nil
}

func renderSchemaStatusTable(fields map[string]*servicepb.MetadataFieldStatus) {
	table := pterm.TableData{
		{"KEY", "TYPE"},
	}

	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, key := range keys {
		field := fields[key]
		table = append(table, []string{
			key,
			cmdutil.MetadataTypeString(field.GetDeclaredType()),
		})
	}

	// Ignore render error — best effort display
	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()
}
