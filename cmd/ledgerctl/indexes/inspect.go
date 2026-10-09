package indexes

import (
	"fmt"
	"io"
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

// InspectRenderOptions supplies host display labels and the already resolved
// metadata schema hint. Datetime values can use the int64 wire encoding.
type InspectRenderOptions struct {
	Ledger       string
	Key          string
	Target       string
	DeclaredType commonpb.MetadataType
}

// RenderInspect writes an inspect response without fetching schema or index
// information. Structured output retains the native response and its cursors.
func RenderInspect(cmd *cobra.Command, resp *servicepb.InspectIndexResponse, options InspectRenderOptions) error {
	text := pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout())

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	text.Println()
	text.Printf("Index: %s on %s (ledger: %s)\n", pterm.Cyan(options.Key), pterm.Cyan(options.Target), pterm.Cyan(options.Ledger))
	text.Println(pterm.Gray("─────────────────────────────────"))

	switch result := resp.GetResult().(type) {
	case *servicepb.InspectIndexResponse_Summary:
		printSummary(result.Summary, options.DeclaredType, cmd.OutOrStdout())
	case *servicepb.InspectIndexResponse_DistinctValues:
		return printDistinctValues(result.DistinctValues, options.DeclaredType, cmd.OutOrStdout())
	case *servicepb.InspectIndexResponse_Facets:
		return printFacets(result.Facets, options.DeclaredType, cmd.OutOrStdout())
	}

	return nil
}

func printSummary(s *servicepb.InspectSummary, declaredType commonpb.MetadataType, writer io.Writer) {
	text := pterm.DefaultBasicText.WithWriter(writer)
	text.Printf("Cardinality:       %d\n", s.GetCardinality())
	text.Printf("Min:               %s\n", formatMetadataValue(s.GetMin(), declaredType))
	text.Printf("Max:               %s\n", formatMetadataValue(s.GetMax(), declaredType))
	text.Printf("Entities with key: %d\n", s.GetEntitiesWithKey())
	text.Printf("Entities null:     %d\n", s.GetEntitiesWithNull())
}

func printDistinctValues(dv *servicepb.InspectDistinctValues, declaredType commonpb.MetadataType, writer io.Writer) error {
	table := pterm.TableData{{"VALUE"}}
	for _, v := range dv.GetValues() {
		table = append(table, []string{formatMetadataValue(v, declaredType)})
	}

	if err := pterm.DefaultTable.WithWriter(writer).WithHasHeader().WithData(table).Render(); err != nil {
		return err
	}

	printInspectCursors(dv.GetPreviousCursor(), dv.GetNextCursor(), writer)

	return nil
}

func printFacets(f *servicepb.InspectFacets, declaredType commonpb.MetadataType, writer io.Writer) error {
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

	if err := pterm.DefaultTable.WithWriter(writer).WithHasHeader().WithData(table).Render(); err != nil {
		return err
	}

	printInspectCursors(f.GetPreviousCursor(), f.GetNextCursor(), writer)

	return nil
}

func printInspectCursors(previous, next string, writer io.Writer) {
	text := pterm.DefaultBasicText.WithWriter(writer)
	if previous == "" && next == "" {
		return
	}

	text.Println()

	if previous != "" {
		text.Printf("Previous page: use --cursor %s\n", pterm.Cyan(previous))
	}

	if next != "" {
		text.Printf("More results available. Use --cursor %s\n", pterm.Cyan(next))
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
