package indexes

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewInspectCommand creates the indexes inspect command.
func NewInspectCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "inspect [flags]",
		Aliases: []string{"i"},
		Short:   "Inspect a metadata index",
		Long: `Scan a metadata index to see distinct values, facets, or a summary.

Examples:
  # Get a summary of the "category" index
  ledgerctl indexes inspect --ledger my-ledger --key category

  # List distinct values
  ledgerctl indexes inspect --ledger my-ledger --key category --mode distinct-values

  # List facets (value + count)
  ledgerctl indexes inspect --ledger my-ledger --key status --mode facets --target transaction`,
		Args: cobra.NoArgs,
		RunE: runInspectIndex,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("key", "", "Metadata key to inspect (required)")
	cmd.Flags().String("target", "account", "Target type: account or transaction")
	cmd.Flags().String("mode", "summary", "Mode: summary, distinct-values, facets")
	cmd.Flags().Uint32("page-size", 20, "Page size for distinct-values/facets")
	cmd.Flags().String("cursor", "", "Pagination cursor from previous response")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	_ = cmd.MarkFlagRequired("key")

	return cmd
}

func runInspectIndex(cmd *cobra.Command, _ []string) error {
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

	key, _ := cmd.Flags().GetString("key")
	target, _ := cmd.Flags().GetString("target")
	mode, _ := cmd.Flags().GetString("mode")
	pageSize, _ := cmd.Flags().GetUint32("page-size")
	cursor, _ := cmd.Flags().GetString("cursor")

	targetType := commonpb.TargetType_TARGET_TYPE_ACCOUNT
	if target == "transaction" {
		targetType = commonpb.TargetType_TARGET_TYPE_TRANSACTION
	}

	var inspectMode servicepb.InspectIndexMode
	switch mode {
	case "distinct-values", "distinctValues":
		inspectMode = servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES
	case "facets":
		inspectMode = servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS
	default:
		inspectMode = servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	resp, err := client.InspectIndex(ctx, &servicepb.InspectIndexRequest{
		Ledger:      ledgerName,
		TargetType:  targetType,
		MetadataKey: key,
		Mode:        inspectMode,
		PageSize:    pageSize,
		Cursor:      cursor,
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to inspect index", err)
	}

	pterm.Println()
	pterm.Printf("Index: %s on %s (ledger: %s)\n", pterm.Cyan(key), pterm.Cyan(target), pterm.Cyan(ledgerName))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	switch result := resp.GetResult().(type) {
	case *servicepb.InspectIndexResponse_Summary:
		printSummary(result.Summary)
	case *servicepb.InspectIndexResponse_DistinctValues:
		printDistinctValues(result.DistinctValues)
	case *servicepb.InspectIndexResponse_Facets:
		printFacets(result.Facets)
	}

	return nil
}

func printSummary(s *servicepb.InspectSummary) {
	pterm.Printf("Cardinality:       %d\n", s.GetCardinality())
	pterm.Printf("Min:               %s\n", formatMetadataValue(s.GetMin()))
	pterm.Printf("Max:               %s\n", formatMetadataValue(s.GetMax()))
	pterm.Printf("Entities with key: %d\n", s.GetEntitiesWithKey())
	pterm.Printf("Entities null:     %d\n", s.GetEntitiesWithNull())
}

func printDistinctValues(dv *servicepb.InspectDistinctValues) {
	table := pterm.TableData{{"VALUE"}}
	for _, v := range dv.GetValues() {
		table = append(table, []string{formatMetadataValue(v)})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()

	if dv.GetHasMore() {
		pterm.Println()
		pterm.Printf("More results available. Use --cursor %s\n", pterm.Cyan(dv.GetNextCursor()))
	}
}

func printFacets(f *servicepb.InspectFacets) {
	facets := make([]*servicepb.InspectFacet, len(f.GetFacets()))
	copy(facets, f.GetFacets())

	sort.Slice(facets, func(i, j int) bool {
		return facets[i].GetCount() > facets[j].GetCount()
	})

	table := pterm.TableData{{"VALUE", "COUNT"}}
	for _, fv := range facets {
		table = append(table, []string{
			formatMetadataValue(fv.GetValue()),
			strconv.FormatUint(fv.GetCount(), 10),
		})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()

	if f.GetHasMore() {
		pterm.Println()
		pterm.Printf("More results available. Use --cursor %s\n", pterm.Cyan(f.GetNextCursor()))
	}
}

func formatMetadataValue(v *commonpb.MetadataValue) string {
	if v == nil {
		return pterm.Gray("(none)")
	}

	switch t := v.GetType().(type) {
	case *commonpb.MetadataValue_StringValue:
		return fmt.Sprintf("%q", t.StringValue)
	case *commonpb.MetadataValue_IntValue:
		return strconv.FormatInt(t.IntValue, 10)
	case *commonpb.MetadataValue_UintValue:
		return strconv.FormatUint(t.UintValue, 10)
	case *commonpb.MetadataValue_BoolValue:
		return strconv.FormatBool(t.BoolValue)
	case *commonpb.MetadataValue_NullValue:
		return pterm.Gray("null")
	default:
		return pterm.Gray("(unknown)")
	}
}
