package ledgers

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounttypes"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewConfigurationCommand creates the ledgers configuration command.
func NewConfigurationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "configuration <name>",
		Aliases: []string{"config", "conf"},
		Short:   "Show a ledger's full configuration",
		Long: `Display all configuration for a ledger: indexes, prepared queries,
and numscript library.

Subcommands:
  export    Export editable configuration as JSON/YAML
  apply     Apply a configuration file (diff-based)

Examples:
  ledgerctl ledgers configuration myledger
  ledgerctl ledgers configuration export myledger --yaml > config.yaml
  ledgerctl ledgers configuration apply myledger -f config.yaml`,
		Args: cobra.ExactArgs(1),
		RunE: runConfiguration,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Bool("expand", false, "Show full content of numscripts and prepared query filters")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	cmd.AddCommand(NewConfigurationExportCommand())
	cmd.AddCommand(NewConfigurationApplyCommand())

	return cmd
}

func runConfiguration(cmd *cobra.Command, args []string) error {
	ledgerName := args[0]

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching configuration for %s...", ledgerName))

	// Fetch ledger info (indexes, metadata schema)
	ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
	if err != nil {
		spinner.Fail("Failed to get ledger")

		return cmdutil.FormatGRPCError("failed to get ledger", err)
	}

	// Fetch prepared queries
	pqResp, err := client.ListPreparedQueries(ctx, &servicepb.ListPreparedQueriesRequest{Ledger: ledgerName})
	if err != nil {
		spinner.Fail("Failed to list prepared queries")

		return cmdutil.FormatGRPCError("failed to list prepared queries", err)
	}

	// Fetch numscripts
	nsStream, err := client.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{Ledger: ledgerName})
	if err != nil {
		spinner.Fail("Failed to list numscripts")

		return cmdutil.FormatGRPCError("failed to list numscripts", err)
	}

	var numscripts []*commonpb.NumscriptInfo
	for {
		info, err := nsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			spinner.Fail("Failed to receive numscripts")

			return cmdutil.FormatGRPCError("failed to receive numscripts", err)
		}
		numscripts = append(numscripts, info)
	}

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, map[string]any{
		"ledger":          ledger,
		"preparedQueries": pqResp.GetQueries(),
		"numscripts":      numscripts,
	}); handled || err != nil {
		return err
	}

	// Header
	pterm.Println()
	pterm.Printf("Configuration for ledger: %s\n", pterm.Cyan(ledgerName))
	pterm.Println(pterm.Gray("═════════════════════════════════════"))

	// 1. Account Types
	renderConfigurationAccountTypes(ledger)

	// 2. Indexes
	renderConfigurationIndexes(ledger)

	expand, _ := cmd.Flags().GetBool("expand")

	// 3. Prepared Queries
	renderConfigurationPreparedQueries(pqResp.GetQueries(), expand)

	// 4. Numscript Library
	renderConfigurationNumscripts(numscripts, expand)

	return nil
}

func renderConfigurationAccountTypes(ledger *commonpb.LedgerInfo) {
	pterm.Println()
	pterm.DefaultSection.Printf("Account Types (%d)\n", len(ledger.GetAccountTypes()))

	if len(ledger.GetAccountTypes()) == 0 {
		pterm.Println(pterm.Gray("  (none)"))

		return
	}

	names := make([]string, 0, len(ledger.GetAccountTypes()))
	for n := range ledger.GetAccountTypes() {
		names = append(names, n)
	}

	sort.Strings(names)

	table := pterm.TableData{
		{"NAME", "PATTERN", "STATUS", "PERSISTENCE"},
	}

	for _, n := range names {
		at := ledger.GetAccountTypes()[n]
		table = append(table, []string{
			at.GetName(),
			at.GetPattern(),
			accounttypes.FormatStatus(at.GetStatus()),
			accounttypes.FormatPersistence(at.GetPersistence()),
		})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()
}

func renderConfigurationIndexes(ledger *commonpb.LedgerInfo) {
	pterm.Println()
	pterm.DefaultSection.Println("Indexes")

	table := pterm.TableData{
		{"TYPE", "TARGET", "KEY"},
	}

	if bi := ledger.GetBuiltinIndexes(); bi != nil {
		if bi.GetReference() {
			table = append(table, []string{"reference", "-", "-"})
		}
		if bi.GetTimestamp() {
			table = append(table, []string{"timestamp", "-", "-"})
		}
		if bi.GetAddress() {
			table = append(table, []string{"address", "-", "-"})
		}
		if bi.GetSourceAddress() {
			table = append(table, []string{"source-address", "-", "-"})
		}
		if bi.GetDestAddress() {
			table = append(table, []string{"dest-address", "-", "-"})
		}
	}

	if schema := ledger.GetMetadataSchema(); schema != nil {
		addMetadataIndexRows(&table, "account", schema.GetAccountFields())
		addMetadataIndexRows(&table, "transaction", schema.GetTransactionFields())
	}

	if len(table) == 1 {
		pterm.Println(pterm.Gray("  (none)"))

		return
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()
}

// addMetadataIndexRows appends rows for indexed metadata fields (config only, no status).
func addMetadataIndexRows(table *pterm.TableData, targetName string, fields map[string]*commonpb.MetadataFieldSchema) {
	keys := make([]string, 0, len(fields))
	for k, f := range fields {
		if f.GetIndexed() {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)

	for _, key := range keys {
		*table = append(*table, []string{"metadata", targetName, key})
	}
}

func renderConfigurationPreparedQueries(queries []*commonpb.PreparedQuery, expand bool) {
	pterm.Println()
	pterm.DefaultSection.Printf("Prepared Queries (%d)\n", len(queries))

	if len(queries) == 0 {
		pterm.Println(pterm.Gray("  (none)"))

		return
	}

	if !expand {
		table := pterm.TableData{
			{"NAME", "TARGET"},
		}
		for _, q := range queries {
			table = append(table, []string{
				q.GetName(),
				queryTargetString(q.GetTarget()),
			})
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()

		return
	}

	for i, q := range queries {
		if i > 0 {
			pterm.Println()
		}
		pterm.Printf("  %s %s\n", pterm.Cyan(q.GetName()), pterm.Gray("─────────────────────────"))
		pterm.Printf("    Name:   %s\n", q.GetName())
		pterm.Printf("    Target: %s\n", queryTargetString(q.GetTarget()))
		pterm.Printf("    Ledger: %s\n", q.GetLedger())

		if q.GetFilter() != nil {
			pterm.Printf("    Filter: %s\n", filterexpr.Format(q.GetFilter()))
		} else {
			pterm.Printf("    Filter: %s\n", pterm.Gray("(none)"))
		}
	}
}

func queryTargetString(target commonpb.QueryTarget) string {
	switch target {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		return "accounts"
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		return "transactions"
	default:
		return "unknown"
	}
}

func renderConfigurationNumscripts(numscripts []*commonpb.NumscriptInfo, expand bool) {
	pterm.Println()
	pterm.DefaultSection.Printf("Numscript Library (%d)\n", len(numscripts))

	if len(numscripts) == 0 {
		pterm.Println(pterm.Gray("  (none)"))

		return
	}

	if !expand {
		table := pterm.TableData{
			{"NAME", "VERSION", "CREATED AT"},
		}
		for _, ns := range numscripts {
			createdAt := ""
			if ns.GetCreatedAt() != nil {
				createdAt = ns.GetCreatedAt().AsTime().Format("2006-01-02T15:04:05Z07:00")
			}
			table = append(table, []string{
				ns.GetName(),
				ns.GetVersion(),
				createdAt,
			})
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()

		return
	}

	for i, ns := range numscripts {
		if i > 0 {
			pterm.Println()
		}
		pterm.Printf("  %s %s\n", pterm.Cyan(ns.GetName()), pterm.Gray("─────────────────────────"))
		pterm.Printf("    Name:       %s\n", ns.GetName())
		pterm.Printf("    Version:    %s\n", ns.GetVersion())
		pterm.Printf("    Ledger:     %s\n", ns.GetLedger())
		if ns.GetCreatedAt() != nil {
			pterm.Printf("    Created At: %s\n", ns.GetCreatedAt().AsTime().Format("2006-01-02T15:04:05Z07:00"))
		}
		pterm.Printf("    Content:\n")
		for line := range strings.SplitSeq(strings.TrimSpace(ns.GetContent()), "\n") {
			pterm.Printf("      %s\n", pterm.Gray(line))
		}
	}
}
