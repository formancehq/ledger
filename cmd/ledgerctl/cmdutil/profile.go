package cmdutil

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

const (
	MetadataKeyQueryProfile       = "x-query-profile"
	MetadataKeyQueryProfileResult = "x-query-profile-result-bin"
)

// AddAnalyzeFlag registers --analyze (with --analyse alias) on the command.
func AddAnalyzeFlag(cmd *cobra.Command) {
	cmd.Flags().Bool("analyze", false, "Display query profile (server-side phase timing, iterator stats)")

	prev := cmd.Flags().GetNormalizeFunc()
	cmd.Flags().SetNormalizeFunc(func(f *pflag.FlagSet, name string) pflag.NormalizedName {
		if name == "analyse" {
			return "analyze"
		}

		if prev != nil {
			return prev(f, name)
		}

		return pflag.NormalizedName(name)
	})
}

// ProfileContext adds the "x-query-profile" metadata to the outgoing context
// so the server will send back profiling information in trailing metadata.
func ProfileContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, MetadataKeyQueryProfile, "true")
}

// ExtractProfile reads the query profile from gRPC trailing metadata.
// The trailer is obtained from the stream after all messages have been received.
func ExtractProfile(trailer metadata.MD) *servicepb.QueryProfile {
	vals := trailer.Get(MetadataKeyQueryProfileResult)
	if len(vals) == 0 {
		return nil
	}

	var profile servicepb.QueryProfile

	err := proto.Unmarshal([]byte(vals[0]), &profile)
	if err != nil {
		return nil
	}

	return &profile
}

// RenderProfile displays a query profile in a human-readable format.
func RenderProfile(profile *servicepb.QueryProfile) {
	if profile == nil {
		pterm.Warning.Println("No profile data received from server.")

		return
	}

	pterm.Println()
	pterm.DefaultHeader.WithBackgroundStyle(pterm.NewStyle(pterm.BgDarkGray)).Println("Query Profile")

	// Server timing first: it is the number that answers "is the server or the
	// network slow?", and the execution breakdown below is a subset of it.
	residual, residualOK := residualDurationUs(profile)

	barrier := formatDurationUs(profile.GetBarrierDurationUs())
	if profile.GetForwarded() {
		// A forwarded read runs its barrier on the remote node, so this 0 means
		// "not measured here", not "no wait happened".
		barrier += " (local only — read was forwarded)"
	}

	serverTiming := pterm.TableData{
		{"Server Phase", "Value"},
		{"Server Duration (consumer-independent)", formatDurationUs(profile.GetServerDurationUs())},
		{"  Prepare (decode/validate/compile)", formatDurationUs(profile.GetPrepareDurationUs())},
		{"  Execute (index+enrich+snapshot)", formatDurationUs(profile.GetExecuteDurationUs())},
		{"  Other server work", formatDurationUs(residual)},
		{"Deliver (serialise + stream write)", formatDurationUs(profile.GetDeliverDurationUs())},
		{"Wall (server + deliver)", formatDurationUs(profile.GetServerDurationUs() + profile.GetDeliverDurationUs())},
		{"Read Barrier (caller-requested wait)", barrier},
		{"Time To First Row", formatDurationUs(profile.GetFirstRowDurationUs())},
	}
	_ = pterm.DefaultTable.WithHasHeader().WithData(serverTiming).Render()

	if !residualOK {
		// The phases cannot exceed the total they decompose; if they do, the
		// server's phase bookkeeping is inconsistent. Say so rather than hiding
		// it behind a clamped 0.
		pterm.Warning.Printfln(
			"Server phase breakdown is inconsistent: prepare (%s) + execute (%s) exceeds the server total (%s). Treat the breakdown as unreliable.",
			formatDurationUs(profile.GetPrepareDurationUs()),
			formatDurationUs(profile.GetExecuteDurationUs()),
			formatDurationUs(profile.GetServerDurationUs()),
		)
	}

	pterm.Println()

	tableData := pterm.TableData{
		{"Execution Metric", "Value"},
		{"Index Duration", formatDurationUs(profile.GetIndexDurationUs())},
		{"Enrichment Duration", formatDurationUs(profile.GetEnrichmentDurationUs())},
		{"Total Duration", formatDurationUs(profile.GetIndexDurationUs() + profile.GetEnrichmentDurationUs())},
		{"Items Collected", strconv.Itoa(int(profile.GetItemsCollected()))},
		{"Enriched Count", strconv.Itoa(int(profile.GetEnrichedCount()))},
		{"Materialized Ranges", strconv.Itoa(int(profile.GetMaterializedRanges()))},
		{"Materialized Items", strconv.Itoa(int(profile.GetMaterializedItems()))},
	}
	_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

	if profile.GetRootIterator() != nil {
		pterm.Println()
		pterm.DefaultSection.Println("Iterator Tree")
		renderIteratorTree(profile.GetRootIterator(), 0)
	}
}

// residualDurationUs is the part of server_duration_us the server did not
// attribute to the prepare or execute phase (response assembly, pagination
// trailer, profile emission). Surfacing it keeps the breakdown honest: a large
// residual means the phase boundaries need refining, not that the time vanished.
//
// The second return is false when the residual came out negative. That is
// impossible if the server's bookkeeping is sound — the phases are windows inside
// the total — so the caller must report it rather than render a clamped 0 that
// looks like a normal reading.
func residualDurationUs(profile *servicepb.QueryProfile) (int64, bool) {
	residual := profile.GetServerDurationUs() - profile.GetPrepareDurationUs() - profile.GetExecuteDurationUs()
	if residual < 0 {
		return 0, false
	}

	return residual, true
}

func formatDurationUs(us int64) string {
	d := time.Duration(us) * time.Microsecond
	if d < time.Millisecond {
		return fmt.Sprintf("%dus", us)
	}

	return fmt.Sprintf("%.2fms", float64(us)/1000.0)
}

func renderIteratorTree(iter *servicepb.IteratorProfile, depth int) {
	indent := strings.Repeat("  ", depth)

	label := iter.GetLabel()
	if label == "" {
		label = iter.GetKind()
	}

	parts := []string{
		fmt.Sprintf("next=%d", iter.GetNextCalls()),
		fmt.Sprintf("seek=%d", iter.GetSeekCalls()),
		fmt.Sprintf("emit=%d", iter.GetItemsEmitted()),
	}

	if iter.GetDurationUs() > 0 {
		parts = append(parts, "dur="+formatDurationUs(iter.GetDurationUs()))

		if selfUs := selfDurationUs(iter); selfUs != iter.GetDurationUs() {
			parts = append(parts, "self="+formatDurationUs(selfUs))
		}
	}

	if iter.GetItemsSkipped() > 0 {
		parts = append(parts, fmt.Sprintf("skip=%d", iter.GetItemsSkipped()))
	}

	if iter.GetMaterializedRanges() > 0 || iter.GetMaterializedItems() > 0 {
		parts = append(parts, fmt.Sprintf("materialized=%d/%d", iter.GetMaterializedRanges(), iter.GetMaterializedItems()))
	}

	if iter.GetBucket() != "" {
		parts = append(parts, "bucket="+iter.GetBucket())
	}

	pterm.Printf("%s%s  %s\n", indent, pterm.Cyan(label), pterm.Gray(strings.Join(parts, " ")))

	for _, child := range iter.GetChildren() {
		renderIteratorTree(child, depth+1)
	}
}

// selfDurationUs returns the duration spent strictly in this node, excluding
// time charged to descendants. Returns the node's own duration when there are
// no children.
func selfDurationUs(iter *servicepb.IteratorProfile) int64 {
	self := iter.GetDurationUs()
	for _, child := range iter.GetChildren() {
		self -= child.GetDurationUs()
	}

	if self < 0 {
		return 0
	}

	return self
}
