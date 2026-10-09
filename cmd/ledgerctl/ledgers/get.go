package ledgers

import (
	"sort"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// NewGetCommand creates the ledgers get command.
func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get <name>",
		Aliases: cmdutil.GetAliases,
		Short:   "Get a ledger by name",
		Long:    "Get detailed information about a ledger by its name via gRPC",
		Args:    cobra.ExactArgs(1),

		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmdutil.AddConsistencyFlags(cmd)
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

// RenderGet writes an already fetched ledger in the native CLI format.
func RenderGet(cmd *cobra.Command, ledger *commonpb.LedgerInfo) error {
	if handled, err := cmdutil.EncodeStructured(cmd, ledger); handled || err != nil {
		return err
	}

	pterm.Println()

	pterm.Printf("Ledger: %s\n", pterm.Cyan(ledger.GetName()))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	pterm.Printf("Name:       %s\n", ledger.GetName())

	createdAt := "-"
	if ledger.GetCreatedAt() != nil {
		createdAt = ledger.GetCreatedAt().AsTime().Format(time.RFC3339)
	}

	pterm.Printf("Created At: %s\n", createdAt)
	pterm.Printf("Mode:       %s\n", ledgerModeString(ledger.GetMode()))

	if ledger.GetMirrorSource() != nil {
		renderMirrorSource(ledger.GetMirrorSource())
	}

	if ledger.GetMirrorSyncProgress() != nil {
		renderMirrorSyncProgress(ledger.GetMirrorSyncProgress())
	}

	if len(ledger.GetAccountTypes()) > 0 {
		renderAccountTypes(ledger.GetAccountTypes())
	}

	if ledger.GetMetadataSchema() != nil {
		renderLedgerSchema(ledger.GetMetadataSchema())
	}

	return nil
}

func renderAccountTypes(types map[string]*commonpb.AccountType) {
	pterm.Println()
	pterm.Println("Account Types:")
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	names := make([]string, 0, len(types))
	for n := range types {
		names = append(names, n)
	}

	sort.Strings(names)

	tableData := pterm.TableData{
		{"  NAME", "PATTERN"},
	}

	for _, n := range names {
		at := types[n]
		tableData = append(tableData, []string{
			"  " + at.GetName(),
			at.GetPattern(),
		})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}

func renderLedgerSchema(schema *commonpb.MetadataSchema) {
	hasAccount := len(schema.GetAccountFields()) > 0
	hasTransaction := len(schema.GetTransactionFields()) > 0

	if !hasAccount && !hasTransaction {
		return
	}

	pterm.Println()
	pterm.Println("Metadata Schema:")
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	if hasAccount {
		pterm.Println("  Account Fields:")
		renderFieldSchemaTable(schema.GetAccountFields())
	}

	if hasTransaction {
		pterm.Println("  Transaction Fields:")
		renderFieldSchemaTable(schema.GetTransactionFields())
	}
}

func renderFieldSchemaTable(fields map[string]*commonpb.MetadataFieldSchema) {
	table := pterm.TableData{
		{"  KEY", "TYPE"},
	}

	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, key := range keys {
		table = append(table, []string{
			"  " + key,
			cmdutil.MetadataTypeString(fields[key].GetType()),
		})
	}

	// Ignore render error — best effort display
	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()
}
