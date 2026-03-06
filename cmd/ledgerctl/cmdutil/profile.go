package cmdutil

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
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

	tableData := pterm.TableData{
		{"Metric", "Value"},
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

	stats := fmt.Sprintf("next=%d seek=%d", iter.GetNextCalls(), iter.GetSeekCalls())
	if iter.GetBucket() != "" {
		stats += " bucket=" + iter.GetBucket()
	}

	pterm.Printf("%s%s  %s\n", indent, pterm.Cyan(label), pterm.Gray(stats))

	for _, child := range iter.GetChildren() {
		renderIteratorTree(child, depth+1)
	}
}
