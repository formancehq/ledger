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
//
// # Two families of measurement
//
// IndexDuration/EnrichmentDuration and the iterator tree describe query
// EXECUTION. The Prepare/Execute/Barrier/Deliver/Server durations describe the
// whole server-side handling of the request, which is strictly larger: request
// decode, filter compilation, response serialisation and stream writes all live
// outside execution and used to be invisible (EN-1859).
//
// # Phase model
//
// The request clock starts when the profile is created — call [WithProfile] as
// the FIRST statement of a read handler, before any decode or validation — and
// stops at [QueryProfile.Finish].
//
//	|<---------------------- wall clock ---------------------->|
//	[ prepare ][ barrier ][ execute ][ residual ][   deliver   ]
//	                 ^                                  ^
//	                 |                                  |
//	     caller-requested wait               consumer-dependent
//	     (excluded from ServerDuration)  (excluded from ServerDuration)
//
// ServerDuration is the headline consumer-independent server cost:
// wall clock - BarrierDuration - DeliverDuration. It is defined identically on
// gRPC and HTTP, which is what makes the two surfaces comparable, and it is the
// only field a server-side latency SLO should be written against.
//
// BarrierDuration is excluded because it is a wait the caller opted into (Raft
// ReadIndex quorum, ReadOptions.min_log_sequence catch-up), not work.
// DeliverDuration is excluded because on a server stream it contains consumer
// back-pressure: folding it in would make the total move with client behaviour
// and mislead exactly the reader trying to decide whether the server is slow.
type QueryProfile struct {
	IndexDuration      time.Duration
	EnrichmentDuration time.Duration
	ItemsCollected     int
	EnrichedCount      int
	MaterializedRanges int
	MaterializedItems  int
	Root               *IteratorStats

	// PrepareDuration covers handler entry to executor invocation:
	// authentication, request decode/validation, filter parsing/compilation and
	// checkpoint-store opening. Barrier waits observed in this window are
	// subtracted out.
	PrepareDuration time.Duration
	// ExecuteDuration covers the executor call(s): snapshot setup, ledger and
	// schema resolution, the index scan (IndexDuration), enrichment
	// (EnrichmentDuration) and, for lazily produced rows, the row pulls
	// reported through AddProduction. Barrier waits observed inside the window
	// are subtracted out.
	ExecuteDuration time.Duration
	// BarrierDuration is time blocked on a caller-requested read-consistency
	// barrier. Excluded from ServerDuration.
	BarrierDuration time.Duration
	// DeliverDuration is time spent serialising result rows and handing them to
	// the transport. Excluded from ServerDuration. On a gRPC server stream it is
	// the sum of the stream.Send() calls and therefore includes consumer
	// back-pressure. Always zero on HTTP, where the profile travels in a
	// response header that must be flushed before the body.
	DeliverDuration time.Duration
	// FirstRowDuration is handler entry to the first row accepted by the
	// transport. Server streams only; zero for unary responses.
	FirstRowDuration time.Duration
	// ServerDuration is the consumer-independent server cost. Computed by
	// Finish; zero until then.
	ServerDuration time.Duration

	// detailed is true when the caller explicitly asked for the profile
	// (gRPC x-query-profile metadata / HTTP X-Query-Profile header). It gates
	// the only instrumentation whose cost scales with the result size: the
	// per-row split between production (execute) and delivery in a streaming
	// send loop. A request that did not ask pays two extra time.Now() calls for
	// the whole loop instead of two per row.
	detailed bool

	// requestStart is the instant the handler began. Zero for a profile built
	// by hand (tests), which turns Finish and the phase marks into no-ops so a
	// hand-built profile keeps whatever durations were assigned to it.
	requestStart time.Time
	// executeStart is non-zero while an execute window is open.
	executeStart time.Time
	// barrierAtExecuteStart snapshots BarrierDuration when the current execute
	// window opened, so a barrier hit inside the executor is not charged to it.
	barrierAtExecuteStart time.Duration
	// executeEntered records whether EnterExecute was ever called, so Finish
	// can attribute an aborted request entirely to the prepare phase.
	executeEntered bool
	// firstRowSeen guards FirstRowDuration against later rows.
	firstRowSeen bool
	// finished makes Finish idempotent, so a handler that both emits the
	// profile and falls through to a shared response writer publishes one
	// stable ServerDuration rather than a second, longer one.
	finished bool
}

// IteratorStats holds per-iterator statistics in the query execution tree.
type IteratorStats struct {
	Label     string // e.g. "PrefixIterator(exist:myledger:a:)"
	Kind      string // "Prefix", "Range", "And", "Or", "Not", "AddressTx", "Reverse", "Slice"
	Prefix    string // Pebble key prefix name (e.g. "exist", "midx")
	NextCalls int64
	SeekCalls int64
	// Duration is inclusive: it covers time spent in children because a parent's
	// Next/SeekGE calls the child's Next/SeekGE while the timer is running.
	// Self-time can be derived at render as Duration - Σ Children.Duration.
	Duration time.Duration
	// ItemsEmitted counts Next() invocations that returned true.
	ItemsEmitted int64
	// MaterializedRanges/MaterializedItems are populated on nodes that wrap a
	// materialized SliceIterator (range scans drained into memory).
	MaterializedRanges int
	MaterializedItems  int
	// ItemsSkipped counts candidates discarded by combinator merge/converge
	// loops. Currently populated by AndIterator only.
	ItemsSkipped int64
	Children     []*IteratorStats
}

type profileKey struct{}

// WithProfile creates a new QueryProfile whose request clock starts now, and
// returns a context carrying it.
//
// Call it as the first statement of a read handler — before authentication,
// request decode, validation and filter compilation — otherwise those phases
// stay outside the measured window and PrepareDuration is meaningless.
//
// detailed must be the transport's "did the caller ask for the profile" answer
// (gRPC x-query-profile metadata, HTTP X-Query-Profile header). It only enables
// per-row phase attribution in streaming send loops; every other measurement is
// O(1) per request and is always collected, because the slow-query log and the
// OTel span consume the same profile and would otherwise lose the phases that
// matter most.
func WithProfile(ctx context.Context, detailed bool) (context.Context, *QueryProfile) {
	p := &QueryProfile{detailed: detailed, requestStart: time.Now()}

	return context.WithValue(ctx, profileKey{}, p), p
}

// Detailed reports whether the caller explicitly requested the profile, which
// authorises instrumentation whose cost scales with the number of result rows.
// Nil-safe.
func (p *QueryProfile) Detailed() bool {
	return p != nil && p.detailed
}

// EnterExecute closes the prepare phase and opens an execution window. Call it
// immediately before invoking the query executor. Nil-safe; a no-op on a
// hand-built profile that has no request clock.
func (p *QueryProfile) EnterExecute() {
	if p == nil || p.requestStart.IsZero() {
		return
	}

	if !p.executeEntered {
		p.executeEntered = true
		// Barrier waits before the executor (min_log_sequence, ReadIndex) are
		// caller-requested waiting, not preparation work.
		p.PrepareDuration = nonNegative(time.Since(p.requestStart) - p.BarrierDuration)
	}

	p.executeStart = time.Now()
	p.barrierAtExecuteStart = p.BarrierDuration
}

// LeaveExecute closes the execution window opened by EnterExecute, charging its
// wall time — minus any barrier wait observed inside it — to ExecuteDuration.
// Nil-safe and safe to call without a matching EnterExecute.
func (p *QueryProfile) LeaveExecute() {
	if p == nil || p.executeStart.IsZero() {
		return
	}

	p.ExecuteDuration += nonNegative(time.Since(p.executeStart) - (p.BarrierDuration - p.barrierAtExecuteStart))
	p.executeStart = time.Time{}
}

// AddBarrierWait records time blocked on a caller-requested read-consistency
// barrier: the Raft ReadIndex quorum round-trip or the min_log_sequence
// read-index catch-up. Nil-safe.
func (p *QueryProfile) AddBarrierWait(d time.Duration) {
	if p == nil {
		return
	}

	p.BarrierDuration += d
}

// AddProduction charges row-production time to the execution phase. Used by
// streaming/drain loops for cursors that produce rows lazily — a follower
// routing to the leader pulls each row over the wire from inside the send loop,
// so that cost is execution, not delivery.
// Nil-safe.
func (p *QueryProfile) AddProduction(d time.Duration) {
	if p == nil {
		return
	}

	p.ExecuteDuration += d
}

// AddDelivery charges row serialisation plus transport hand-off to the delivery
// phase, which is excluded from ServerDuration. Nil-safe.
func (p *QueryProfile) AddDelivery(d time.Duration) {
	if p == nil {
		return
	}

	p.DeliverDuration += d
}

// MarkFirstRow records the instant the first result row was accepted by the
// transport. Subsequent calls are ignored. Nil-safe.
func (p *QueryProfile) MarkFirstRow() {
	if p == nil || p.requestStart.IsZero() || p.firstRowSeen {
		return
	}

	p.firstRowSeen = true
	p.FirstRowDuration = time.Since(p.requestStart)
}

// Finish stops the request clock and computes ServerDuration. Call it once,
// after the response has been handed to the transport and before the profile is
// serialised, logged or emitted to a span. Nil-safe and idempotent.
func (p *QueryProfile) Finish() {
	if p == nil || p.requestStart.IsZero() || p.finished {
		return
	}

	p.finished = true

	p.LeaveExecute()

	p.ServerDuration = nonNegative(time.Since(p.requestStart) - p.BarrierDuration - p.DeliverDuration)

	if !p.executeEntered {
		// The request never reached the executor (validation rejection, missing
		// ledger, …): everything measured was preparation.
		p.PrepareDuration = p.ServerDuration
	}
}

// nonNegative clamps a phase duration at zero. A negative value can only come
// from an accumulator being credited more than the enclosing window actually
// lasted (a double-counted barrier wait, say), and publishing a negative
// duration would be worse than publishing a floor.
func nonNegative(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}

	return d
}

// ProfileFromContext extracts the QueryProfile from the context.
// Returns nil if no profile was set.
func ProfileFromContext(ctx context.Context) *QueryProfile {
	p, _ := ctx.Value(profileKey{}).(*QueryProfile)

	return p
}

// TotalDuration returns the sum of index and enrichment durations, i.e. the
// query EXECUTION total. It is a subset of ServerDuration and must not be used
// as "how long the server took" — use ServerDuration for that.
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
		ServerDurationUs:     p.ServerDuration.Microseconds(),
		PrepareDurationUs:    p.PrepareDuration.Microseconds(),
		ExecuteDurationUs:    p.ExecuteDuration.Microseconds(),
		BarrierDurationUs:    p.BarrierDuration.Microseconds(),
		DeliverDurationUs:    p.DeliverDuration.Microseconds(),
		FirstRowDurationUs:   p.FirstRowDuration.Microseconds(),
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
		Label:              s.Label,
		Kind:               s.Kind,
		Bucket:             s.Prefix,
		NextCalls:          s.NextCalls,
		SeekCalls:          s.SeekCalls,
		DurationUs:         s.Duration.Microseconds(),
		ItemsEmitted:       s.ItemsEmitted,
		MaterializedRanges: int32(s.MaterializedRanges),
		MaterializedItems:  int64(s.MaterializedItems),
		ItemsSkipped:       s.ItemsSkipped,
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
		attribute.Int64("query.server_duration_us", p.ServerDuration.Microseconds()),
		attribute.Int64("query.prepare_duration_us", p.PrepareDuration.Microseconds()),
		attribute.Int64("query.execute_duration_us", p.ExecuteDuration.Microseconds()),
		attribute.Int64("query.barrier_duration_us", p.BarrierDuration.Microseconds()),
		attribute.Int64("query.deliver_duration_us", p.DeliverDuration.Microseconds()),
		attribute.Int64("query.first_row_duration_us", p.FirstRowDuration.Microseconds()),
		attribute.Bool("query.profile_detailed", p.detailed),
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

	if logger.Enabled(logging.TraceLevel) {
		fields := map[string]any{
			"indexDurationUs":      p.IndexDuration.Microseconds(),
			"enrichmentDurationUs": p.EnrichmentDuration.Microseconds(),
			"itemsCollected":       p.ItemsCollected,
			"enrichedCount":        p.EnrichedCount,
			"materializedRanges":   p.MaterializedRanges,
			"materializedItems":    p.MaterializedItems,
			"serverDurationUs":     p.ServerDuration.Microseconds(),
			"prepareDurationUs":    p.PrepareDuration.Microseconds(),
			"executeDurationUs":    p.ExecuteDuration.Microseconds(),
			"barrierDurationUs":    p.BarrierDuration.Microseconds(),
			"deliverDurationUs":    p.DeliverDuration.Microseconds(),
			"firstRowDurationUs":   p.FirstRowDuration.Microseconds(),
			// False means the caller did not ask for the profile, so the
			// execute/deliver split was not attributed per row: the whole
			// streaming loop was charged to deliverDurationUs.
			"profileDetailed": p.detailed,
		}
		if p.Root != nil {
			fields["iteratorTree"] = p.Root.String()
		}

		logger.WithFields(fields).Tracef("Query profile (server=%s, execution=%s)", p.ServerDuration, p.TotalDuration())
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
	fmt.Fprintf(b, "%s%s next=%d seek=%d emit=%d", indent, s.Label, s.NextCalls, s.SeekCalls, s.ItemsEmitted)

	if s.Duration > 0 {
		fmt.Fprintf(b, " dur=%s", s.Duration)
	}

	if s.ItemsSkipped > 0 {
		fmt.Fprintf(b, " skip=%d", s.ItemsSkipped)
	}

	if s.MaterializedRanges > 0 || s.MaterializedItems > 0 {
		fmt.Fprintf(b, " materialized=%d/%d", s.MaterializedRanges, s.MaterializedItems)
	}

	if s.Prefix != "" {
		fmt.Fprintf(b, " bucket=%s", s.Prefix)
	}

	b.WriteByte('\n')

	for _, child := range s.Children {
		child.writeIndented(b, depth+1)
	}
}
