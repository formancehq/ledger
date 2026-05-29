package query

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// QueryProfile collects execution statistics for a read query.
// A single instance is created per query and threaded through the context.
// All fields are updated synchronously (no concurrency within a single query).
type QueryProfile struct {
	IndexDuration      time.Duration
	EnrichmentDuration time.Duration
	ItemsCollected     int
	EnrichedCount      int
	MaterializedRanges int
	MaterializedItems  int
	Root               *IteratorStats
}

// IteratorStats holds per-iterator statistics in the query execution tree.
type IteratorStats struct {
	Label     string // e.g. "PrefixIterator(exist:myledger:a:)"
	Kind      string // "Prefix", "Range", "And", "Or", "Not", "AddressTx", "Reverse", "Slice"
	Prefix    string // Pebble key prefix name (e.g. "exist", "midx")
	NextCalls int64
	SeekCalls int64
	Children  []*IteratorStats
}

type profileKey struct{}

// WithProfile creates a new QueryProfile and returns a context containing it.
func WithProfile(ctx context.Context) (context.Context, *QueryProfile) {
	p := &QueryProfile{}

	return context.WithValue(ctx, profileKey{}, p), p
}

// ProfileFromContext extracts the QueryProfile from the context.
// Returns nil if no profile was set.
func ProfileFromContext(ctx context.Context) *QueryProfile {
	p, _ := ctx.Value(profileKey{}).(*QueryProfile)

	return p
}

// TotalDuration returns the sum of index and enrichment durations.
func (p *QueryProfile) TotalDuration() time.Duration {
	return p.IndexDuration + p.EnrichmentDuration
}

// ToProto converts the profile to its protobuf representation.
func (p *QueryProfile) ToProto() *servicepb.QueryProfile {
	if p == nil {
		return nil
	}

	pb := &servicepb.QueryProfile{
		IndexDurationUs:      p.IndexDuration.Microseconds(),
		EnrichmentDurationUs: p.EnrichmentDuration.Microseconds(),
		ItemsCollected:       int32(p.ItemsCollected),
		EnrichedCount:        int32(p.EnrichedCount),
		MaterializedRanges:   int32(p.MaterializedRanges),
		MaterializedItems:    int32(p.MaterializedItems),
	}
	if p.Root != nil {
		pb.RootIterator = p.Root.ToProto()
	}

	return pb
}

// ToProto converts iterator stats to protobuf.
func (s *IteratorStats) ToProto() *servicepb.IteratorProfile {
	if s == nil {
		return nil
	}

	pb := &servicepb.IteratorProfile{
		Label:     s.Label,
		Kind:      s.Kind,
		Bucket:    s.Prefix,
		NextCalls: s.NextCalls,
		SeekCalls: s.SeekCalls,
	}
	for _, child := range s.Children {
		pb.Children = append(pb.Children, child.ToProto())
	}

	return pb
}

// EmitToSpan sets OTel span attributes from the profile.
// Only call this when the query exceeded the profiling threshold.
func (p *QueryProfile) EmitToSpan(span trace.Span) {
	if p == nil || !span.IsRecording() {
		return
	}

	span.SetAttributes(
		attribute.Int64("query.index_duration_us", p.IndexDuration.Microseconds()),
		attribute.Int64("query.enrichment_duration_us", p.EnrichmentDuration.Microseconds()),
		attribute.Int("query.items_collected", p.ItemsCollected),
		attribute.Int("query.enriched_count", p.EnrichedCount),
		attribute.Int("query.materialized_ranges", p.MaterializedRanges),
		attribute.Int("query.materialized_items", p.MaterializedItems),
	)

	if p.Root != nil {
		span.SetAttributes(attribute.String("query.iterator_tree", p.Root.String()))
	}
}

// LogTo emits a structured debug log with the profile data.
func (p *QueryProfile) LogTo(logger logging.Logger) {
	if p == nil {
		return
	}

	if logger.Enabled(logging.DebugLevel) {
		fields := map[string]any{
			"indexDurationUs":      p.IndexDuration.Microseconds(),
			"enrichmentDurationUs": p.EnrichmentDuration.Microseconds(),
			"itemsCollected":       p.ItemsCollected,
			"enrichedCount":        p.EnrichedCount,
			"materializedRanges":   p.MaterializedRanges,
			"materializedItems":    p.MaterializedItems,
		}
		if p.Root != nil {
			fields["iteratorTree"] = p.Root.String()
		}

		logger.WithFields(fields).Debugf("Query profile (total=%s)", p.TotalDuration())
	}
}

// String returns a human-readable indented tree of the iterator stats.
func (s *IteratorStats) String() string {
	if s == nil {
		return "<nil>"
	}

	var b strings.Builder
	s.writeIndented(&b, 0)

	return b.String()
}

func (s *IteratorStats) writeIndented(b *strings.Builder, depth int) {
	indent := strings.Repeat("  ", depth)
	fmt.Fprintf(b, "%s%s next=%d seek=%d", indent, s.Label, s.NextCalls, s.SeekCalls)

	if s.Prefix != "" {
		fmt.Fprintf(b, " bucket=%s", s.Prefix)
	}

	b.WriteByte('\n')

	for _, child := range s.Children {
		child.writeIndented(b, depth+1)
	}
}
