package indexes

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewInspectCommand creates the indexes inspect command.
func NewInspectCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "inspect [flags]",
		Aliases: cmdutil.InspectAliases,
		Short:   "Inspect a metadata index",
		Long: `Scan a metadata index to see distinct values, facets, or a summary.

Examples:
  # Get a summary of the "category" index
  ledgerctl indexes inspect --ledger my-ledger --key category

  # List distinct values
  ledgerctl indexes inspect --ledger my-ledger --key category --mode distinct-values

  # List facets (value + count)
  ledgerctl indexes inspect --ledger my-ledger --key status --mode facets --target transaction`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runInspectIndex,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("key", "", "Metadata key to inspect (required)")
	cmd.Flags().String("target", "account", "Target type: account or transaction")
	cmdutil.RegisterEnumCompletion(cmd, "target", "account", "transaction")
	cmd.Flags().String("mode", "summary", "Mode: summary, distinct-values, facets")
	cmdutil.RegisterEnumCompletion(cmd, "mode", "summary", "distinct-values", "facets")
	cmd.Flags().Uint32("page-size", 20, "Page size for distinct-values/facets")
	cmd.Flags().String("cursor", "", "Pagination cursor from previous response")
	cmdutil.AddOutputFlags(cmd)
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

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	declaredType := declaredMetadataType(ctx, client, ledgerName, targetType, key)

	pterm.Println()
	pterm.Printf("Index: %s on %s (ledger: %s)\n", pterm.Cyan(key), pterm.Cyan(target), pterm.Cyan(ledgerName))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	switch result := resp.GetResult().(type) {
	case *servicepb.InspectIndexResponse_Summary:
		printSummary(result.Summary, declaredType)
	case *servicepb.InspectIndexResponse_DistinctValues:
		printDistinctValues(result.DistinctValues, declaredType)
	case *servicepb.InspectIndexResponse_Facets:
		printFacets(result.Facets, declaredType)
	}

	return nil
}

// declaredMetadataType resolves the schema-declared MetadataType for
// (ledger, targetType, key), or METADATA_TYPE_STRING when the lookup fails or
// the key has no declaration. It is a render hint only: datetime index keys
// share the int64 encoding, so the server returns an int_value for them, and
// formatMetadataValue uses this type to render those values as RFC3339 instead
// of raw microseconds (mirroring the HTTP inspect handler). A failed lookup
// degrades to the default integer rendering rather than erroring.
func declaredMetadataType(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	ledgerName string,
	targetType commonpb.TargetType,
	key string,
) commonpb.MetadataType {
	ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
	if err != nil {
		return commonpb.MetadataType_METADATA_TYPE_STRING
	}

	_, fs := commonpb.SchemaFieldForTarget(ledger.GetMetadataSchema(), targetType, key)

	return fs.GetType()
}

func printSummary(s *servicepb.InspectSummary, declaredType commonpb.MetadataType) {
	pterm.Printf("Cardinality:       %d\n", s.GetCardinality())
	pterm.Printf("Min:               %s\n", formatMetadataValue(s.GetMin(), declaredType))
	pterm.Printf("Max:               %s\n", formatMetadataValue(s.GetMax(), declaredType))
	pterm.Printf("Entities with key: %d\n", s.GetEntitiesWithKey())
	pterm.Printf("Entities null:     %d\n", s.GetEntitiesWithNull())
}

func printDistinctValues(dv *servicepb.InspectDistinctValues, declaredType commonpb.MetadataType) {
	table := pterm.TableData{{"VALUE"}}
	for _, v := range dv.GetValues() {
		table = append(table, []string{formatMetadataValue(v, declaredType)})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()

	if dv.GetHasMore() {
		pterm.Println()
		pterm.Printf("More results available. Use --cursor %s\n", pterm.Cyan(dv.GetNextCursor()))
	}
}

func printFacets(f *servicepb.InspectFacets, declaredType commonpb.MetadataType) {
	facets := make([]*servicepb.InspectFacet, len(f.GetFacets()))
	copy(facets, f.GetFacets())

	sort.Slice(facets, func(i, j int) bool {
		return facets[i].GetCount() > facets[j].GetCount()
	})

	table := pterm.TableData{{"VALUE", "COUNT"}}
	for _, fv := range facets {
		table = append(table, []string{
			formatMetadataValue(fv.GetValue(), declaredType),
			strconv.FormatUint(fv.GetCount(), 10),
		})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()

	if f.GetHasMore() {
		pterm.Println()
		pterm.Printf("More results available. Use --cursor %s\n", pterm.Cyan(f.GetNextCursor()))
	}
}

func formatMetadataValue(v *commonpb.MetadataValue, declaredType commonpb.MetadataType) string {
	if v == nil {
		return pterm.Gray("(none)")
	}

	switch t := v.GetType().(type) {
	case *commonpb.MetadataValue_StringValue:
		return fmt.Sprintf("%q", t.StringValue)
	case *commonpb.MetadataValue_IntValue:
		// Datetime index keys share the int64 encoding, so the server returns an
		// int_value for them; render as RFC3339 when the field is declared datetime.
		if commonpb.IsDatetimeType(declaredType) {
			return time.UnixMicro(t.IntValue).UTC().Format(time.RFC3339Nano)
		}

		return strconv.FormatInt(t.IntValue, 10)
	case *commonpb.MetadataValue_UintValue:
		return strconv.FormatUint(t.UintValue, 10)
	case *commonpb.MetadataValue_DatetimeValue:
		return commonpb.MetadataValueToString(v)
	case *commonpb.MetadataValue_BoolValue:
		return strconv.FormatBool(t.BoolValue)
	case *commonpb.MetadataValue_NullValue:
		return pterm.Gray("null")
	default:
		return pterm.Gray("(unknown)")
	}
}
