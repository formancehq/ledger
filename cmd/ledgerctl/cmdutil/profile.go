package cmdutil

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	MetadataKeyQueryProfile       = "x-query-profile"
	MetadataKeyQueryProfileResult = "x-query-profile-result-bin"
)

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
	if err := proto.Unmarshal([]byte(vals[0]), &profile); err != nil {
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

	tableData := pterm.TableData{
		{"Metric", "Value"},
		{"Index Duration", formatDurationUs(profile.IndexDurationUs)},
		{"Enrichment Duration", formatDurationUs(profile.EnrichmentDurationUs)},
		{"Total Duration", formatDurationUs(profile.IndexDurationUs + profile.EnrichmentDurationUs)},
		{"Items Collected", fmt.Sprintf("%d", profile.ItemsCollected)},
		{"Enriched Count", fmt.Sprintf("%d", profile.EnrichedCount)},
		{"Materialized Ranges", fmt.Sprintf("%d", profile.MaterializedRanges)},
		{"Materialized Items", fmt.Sprintf("%d", profile.MaterializedItems)},
	}
	_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

	if profile.RootIterator != nil {
		pterm.Println()
		pterm.DefaultSection.Println("Iterator Tree")
		renderIteratorTree(profile.RootIterator, 0)
	}
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
	label := iter.Label
	if label == "" {
		label = iter.Kind
	}

	stats := fmt.Sprintf("next=%d seek=%d", iter.NextCalls, iter.SeekCalls)
	if iter.Bucket != "" {
		stats += fmt.Sprintf(" bucket=%s", iter.Bucket)
	}

	pterm.Printf("%s%s  %s\n", indent, pterm.Cyan(label), pterm.Gray(stats))

	for _, child := range iter.Children {
		renderIteratorTree(child, depth+1)
	}
}
