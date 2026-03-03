package ledgers

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
)

// NewCatalogCommand creates the ledgers catalog command.
func NewCatalogCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "catalog <name>",
		Aliases: []string{"cat"},
		Short:   "Show a ledger's full configuration catalog",
		Long: `Display all configuration for a ledger: chart of accounts, indexes,
prepared queries, and numscript library.

Examples:
  ledgerctl ledgers catalog myledger`,
		Args: cobra.ExactArgs(1),
		RunE: runCatalog,
	}

	cmd.Flags().Bool("expand", false, "Show full content of numscripts and prepared query filters")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runCatalog(cmd *cobra.Command, args []string) error {
	ledgerName := args[0]

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching catalog for %s...", ledgerName))

	// Fetch ledger info (chart of accounts, indexes, metadata schema)
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

	// Header
	pterm.Println()
	pterm.Printf("Catalog for ledger: %s\n", pterm.Cyan(ledgerName))
	pterm.Println(pterm.Gray("═════════════════════════════════════"))

	// 1. Chart of Accounts
	renderCatalogChart(ledger)

	// 2. Indexes
	renderCatalogIndexes(ledger)

	expand, _ := cmd.Flags().GetBool("expand")

	// 3. Prepared Queries
	renderCatalogPreparedQueries(pqResp.Queries, expand)

	// 4. Numscript Library
	renderCatalogNumscripts(numscripts, expand)

	return nil
}

func renderCatalogChart(ledger *commonpb.LedgerInfo) {
	pterm.Println()
	mode := chartEnforcementModeString(ledger.EnforcementMode)
	pterm.DefaultSection.Println("Chart of Accounts (" + mode + ")")

	if ledger.ChartOfAccounts == nil || len(ledger.ChartOfAccounts.Roots) == 0 {
		pterm.Println(pterm.Gray("  (none)"))
		return
	}

	renderChartTree(ledger.ChartOfAccounts.Roots, "  ")
}

func chartEnforcementModeString(mode commonpb.ChartEnforcementMode) string {
	switch mode {
	case commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT:
		return "STRICT"
	case commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT:
		return "AUDIT"
	default:
		return "OFF"
	}
}

// renderChartTree recursively renders the chart of accounts as an indented tree.
func renderChartTree(segments map[string]*commonpb.ChartSegment, indent string) {
	keys := make([]string, 0, len(segments))
	for k := range segments {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, name := range keys {
		seg := segments[name]
		label := name
		if seg.Account {
			label += pterm.Gray("  (account)")
		}
		pterm.Println(indent + label)

		// Render variable child if present
		if seg.Variable != nil {
			renderChartVariable(seg.Variable, indent+"  ")
		}

		// Render fixed children
		if len(seg.Children) > 0 {
			renderChartTree(seg.Children, indent+"  ")
		}
	}
}

// renderChartVariable renders a variable segment.
func renderChartVariable(v *commonpb.ChartVariable, indent string) {
	var parts []string
	parts = append(parts, ":"+v.Name)
	if v.Pattern != "" {
		parts = append(parts, pterm.Gray(v.Pattern))
	}
	if v.Account {
		parts = append(parts, pterm.Gray("(account)"))
	}
	pterm.Println(indent + strings.Join(parts, " "))

	// Recurse into variable's children
	if v.Variable != nil {
		renderChartVariable(v.Variable, indent+"  ")
	}
	if len(v.Children) > 0 {
		renderChartTree(v.Children, indent+"  ")
	}
}

func renderCatalogIndexes(ledger *commonpb.LedgerInfo) {
	pterm.Println()
	pterm.DefaultSection.Println("Indexes")

	table := pterm.TableData{
		{"TYPE", "TARGET", "KEY", "STATUS"},
	}

	if bi := ledger.BuiltinIndexes; bi != nil {
		if bi.Reference {
			table = append(table, []string{"reference", "-", "-", indexBuildStatusString(bi.ReferenceStatus)})
		}
		if bi.Timestamp {
			table = append(table, []string{"timestamp", "-", "-", indexBuildStatusString(bi.TimestampStatus)})
		}
		if bi.Address {
			table = append(table, []string{"address", "-", "-", indexBuildStatusString(bi.AddressStatus)})
		}
		if bi.SourceAddress {
			table = append(table, []string{"source-address", "-", "-", indexBuildStatusString(bi.SourceAddressStatus)})
		}
		if bi.DestAddress {
			table = append(table, []string{"dest-address", "-", "-", indexBuildStatusString(bi.DestAddressStatus)})
		}
	}

	if schema := ledger.MetadataSchema; schema != nil {
		addMetadataIndexRowsWithProgress(&table, "account", schema.AccountFields, nil, 0, commonpb.TargetType_TARGET_TYPE_ACCOUNT)
		addMetadataIndexRowsWithProgress(&table, "transaction", schema.TransactionFields, nil, 0, commonpb.TargetType_TARGET_TYPE_TRANSACTION)
	}

	if len(table) == 1 {
		pterm.Println(pterm.Gray("  (none)"))
		return
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()
}

func renderCatalogPreparedQueries(queries []*commonpb.PreparedQuery, expand bool) {
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

	marshaler := protojson.MarshalOptions{Indent: "  ", EmitUnpopulated: false}
	for i, q := range queries {
		if i > 0 {
			pterm.Println()
		}
		pterm.Printf("  %s %s\n", pterm.Cyan(q.Name), pterm.Gray("─────────────────────────"))
		pterm.Printf("    Name:   %s\n", q.Name)
		pterm.Printf("    Target: %s\n", queryTargetString(q.Target))
		pterm.Printf("    Ledger: %s\n", q.Ledger)

		if q.Filter != nil {
			pterm.Printf("    Filter:\n")
			filterJSON, err := marshaler.Marshal(q.Filter)
			if err != nil {
				pterm.Printf("      %s\n", pterm.Red("(failed to marshal filter)"))
			} else {
				for _, line := range strings.Split(string(filterJSON), "\n") {
					pterm.Printf("      %s\n", line)
				}
			}
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

func renderCatalogNumscripts(numscripts []*commonpb.NumscriptInfo, expand bool) {
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
