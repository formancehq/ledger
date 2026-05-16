package ledgers

import (
	"fmt"
	"sort"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewGetSchemaCommand creates the ledgers get-schema command.
func NewGetSchemaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get-schema <name>",
		Aliases: []string{"schema", "gs"},
		Short:   "Get metadata schema status for a ledger",
		Long: `Display the metadata schema for a ledger including conversion status.

Shows tables (Account Fields, Transaction Fields, Ledger Fields) with columns: KEY, TYPE, STATUS.
The status shows COMPLETE or CONVERTING.

Examples:
  ledgerctl ledgers get-schema my-ledger
  ledgerctl ledgers schema my-ledger
  ledgerctl ledgers gs my-ledger --json`,
		Args: cobra.ExactArgs(1),
		RunE: runGetSchema,
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
		{"KEY", "TYPE", "STATUS"},
	}

	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, key := range keys {
		field := fields[key]
		status := conversionStatusString(field.GetStatus(), field.GetTotalKeys(), field.GetConvertedKeys())
		table = append(table, []string{
			key,
			cmdutil.MetadataTypeString(field.GetDeclaredType()),
			status,
		})
	}

	// Ignore render error — best effort display
	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()
}

func conversionStatusString(s commonpb.MetadataConversionStatus, totalKeys, convertedKeys uint64) string {
	switch s {
	case commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE:
		return pterm.Green("COMPLETE")
	case commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING:
		if totalKeys > 0 {
			return pterm.Yellow(fmt.Sprintf("CONVERTING (%d/%d)", convertedKeys, totalKeys))
		}

		return pterm.Yellow("CONVERTING")
	default:
		return s.String()
	}
}
