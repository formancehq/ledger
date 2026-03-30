package ledgers

import (
	"fmt"
	"sort"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/accounttypes"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewGetCommand creates the ledgers get command.
func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get <name>",
		Aliases: []string{"g", "show", "describe"},
		Short:   "Get a ledger by name",
		Long:    "Get detailed information about a ledger by its name via gRPC",
		Args:    cobra.ExactArgs(1),
		RunE:    runGet,
	}

	cmd.Flags().Uint64("checkpoint-id", 0, "Read from a query checkpoint instead of the live store")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runGet(cmd *cobra.Command, args []string) error {
	ledgerName := args[0]

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	checkpointID, _ := cmd.Flags().GetUint64("checkpoint-id")

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching ledger %s...", ledgerName))

	ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
		Ledger:       ledgerName,
		CheckpointId: checkpointID,
	})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get ledger", err)
	}

	_ = spinner.Stop()

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
		{"  NAME", "PATTERN", "STATUS"},
	}

	for _, n := range names {
		at := types[n]
		tableData = append(tableData, []string{
			"  " + at.GetName(),
			at.GetPattern(),
			accounttypes.FormatStatus(at.GetStatus()),
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
