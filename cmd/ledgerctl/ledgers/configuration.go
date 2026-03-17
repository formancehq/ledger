package ledgers

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/filterexpr"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewConfigurationCommand creates the ledgers configuration command.
func NewConfigurationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "configuration <name>",
		Aliases: []string{"config", "conf"},
		Short:   "Show a ledger's full configuration",
		Long: `Display all configuration for a ledger: indexes, prepared queries,
and numscript library.

Examples:
  ledgerctl ledgers configuration myledger`,
		Args: cobra.ExactArgs(1),
		RunE: runConfiguration,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Bool("expand", false, "Show full content of numscripts and prepared query filters")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

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
		if err == io.EOF {
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
		"preparedQueries": pqResp.Queries,
		"numscripts":      numscripts,
	}); handled || err != nil {
		return err
	}

	// Header
	pterm.Println()
	pterm.Printf("Configuration for ledger: %s\n", pterm.Cyan(ledgerName))
	pterm.Println(pterm.Gray("═════════════════════════════════════"))

	// 1. Indexes
	renderConfigurationIndexes(ledger)

	expand, _ := cmd.Flags().GetBool("expand")

	// 2. Prepared Queries
	renderConfigurationPreparedQueries(pqResp.Queries, expand)

	// 3. Numscript Library
	renderConfigurationNumscripts(numscripts, expand)

	return nil
}

func renderConfigurationIndexes(ledger *commonpb.LedgerInfo) {
	pterm.Println()
	pterm.DefaultSection.Println("Indexes")

	table := pterm.TableData{
		{"TYPE", "TARGET", "KEY"},
	}

	if bi := ledger.BuiltinIndexes; bi != nil {
		if bi.Reference {
			table = append(table, []string{"reference", "-", "-"})
		}
		if bi.Timestamp {
			table = append(table, []string{"timestamp", "-", "-"})
		}
		if bi.Address {
			table = append(table, []string{"address", "-", "-"})
		}
		if bi.SourceAddress {
			table = append(table, []string{"source-address", "-", "-"})
		}
		if bi.DestAddress {
			table = append(table, []string{"dest-address", "-", "-"})
		}
	}

	if schema := ledger.MetadataSchema; schema != nil {
		addMetadataIndexRows(&table, "account", schema.AccountFields)
		addMetadataIndexRows(&table, "transaction", schema.TransactionFields)
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
				q.Name,
				queryTargetString(q.Target),
			})
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()
		return
	}

	for i, q := range queries {
		if i > 0 {
			pterm.Println()
		}
		pterm.Printf("  %s %s\n", pterm.Cyan(q.Name), pterm.Gray("─────────────────────────"))
		pterm.Printf("    Name:   %s\n", q.Name)
		pterm.Printf("    Target: %s\n", queryTargetString(q.Target))
		pterm.Printf("    Ledger: %s\n", q.Ledger)

		if q.Filter != nil {
			pterm.Printf("    Filter: %s\n", filterexpr.Format(q.Filter))
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
			if ns.CreatedAt != nil {
				createdAt = ns.CreatedAt.AsTime().Format("2006-01-02T15:04:05Z07:00")
			}
			table = append(table, []string{
				ns.Name,
				ns.Version,
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
		pterm.Printf("  %s %s\n", pterm.Cyan(ns.Name), pterm.Gray("─────────────────────────"))
		pterm.Printf("    Name:       %s\n", ns.Name)
		pterm.Printf("    Version:    %s\n", ns.Version)
		pterm.Printf("    Ledger:     %s\n", ns.Ledger)
		if ns.CreatedAt != nil {
			pterm.Printf("    Created At: %s\n", ns.CreatedAt.AsTime().Format("2006-01-02T15:04:05Z07:00"))
		}
		pterm.Printf("    Content:\n")
		for _, line := range strings.Split(strings.TrimSpace(ns.Content), "\n") {
			pterm.Printf("      %s\n", pterm.Gray(line))
		}
	}
}
