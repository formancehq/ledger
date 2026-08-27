package query

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
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
// the FIRST statement of a read handler, before any validation, or
// [WithProfileStartingAt] from a transport that can stamp an earlier instant —
// and stops at [QueryProfile.Finish].
//
//	|<---------------------- wall clock ---------------------->|
//	[ prepare ][ barrier ][ execute ][ residual ][   deliver   ]
//	                 ^                                  ^
//	                 |                                  |
//	     caller-requested wait               consumer-dependent
//	     (excluded from ServerDuration)  (excluded from ServerDuration)
//
// There are two totals, and which one to use depends on the question:
//
//   - ServerDuration = wall clock - BarrierDuration - DeliverDuration. The
//     consumer-independent number: it cannot be moved by a slow client. Defined
//     identically on gRPC and HTTP, so the two surfaces are comparable. It does
//     NOT include row serialisation, which on a gRPC server stream is
//     inseparable from the transport write (see WallDuration).
//   - WallDuration = ServerDuration + DeliverDuration. Everything the server
//     spent on the request except the caller-requested barrier, including row
//     serialisation — but on a stream it also absorbs consumer back-pressure.
//
// BarrierDuration is excluded from both because it is a wait the caller opted
// into (Raft ReadIndex quorum, ReadOptions.min_log_sequence catch-up), not work.
//
// DeliverDuration is excluded from ServerDuration because on a server stream it
// contains consumer back-pressure: folding it in would make the headline total
// move with client behaviour and mislead the reader trying to decide whether the
// server is slow. It is NOT excluded from the slow-query threshold, which uses
// WallDuration precisely so that serialisation cost and forwarded-read cost
// cannot hide from it.
type QueryProfile struct {
	IndexDuration      time.Duration
	EnrichmentDuration time.Duration
	ItemsCollected     int
	EnrichedCount      int
	MaterializedRanges int
	MaterializedItems  int
	Root               *IteratorStats

	// PrepareDuration covers request entry to executor invocation:
	// authentication, request validation, filter parsing/compilation and
	// checkpoint-store opening, plus query/body decoding on HTTP. gRPC protobuf
	// decode is outside it — grpc-go unmarshals the request before dispatching to
	// the handler, so no in-handler clock can reach it. Barrier waits observed in
	// this window are subtracted out.
	PrepareDuration time.Duration
	// ExecuteDuration covers the executor call(s): snapshot setup, ledger and
	// schema resolution, the index scan (IndexDuration), enrichment
	// (EnrichmentDuration) and, for lazily produced rows, the row pulls
	// reported through AddProduction. Barrier waits observed inside the window
	// are subtracted out.
	ExecuteDuration time.Duration
	// BarrierDuration is time blocked on caller-requested read-consistency waits,
	// LOCAL to this node: the min_log_sequence catch-up and the Raft ReadIndex
	// quorum. Excluded from ServerDuration. Every local wait counts, including
	// one that fails or is superseded — a syncing follower burns a quorum wait
	// before falling back to the leader and reports it here, with Forwarded true.
	BarrierDuration time.Duration
	// DeliverDuration is time spent serialising result rows and handing them to
	// the transport. Excluded from ServerDuration. On a gRPC server stream it is
	// the sum of the stream.Send() calls and therefore includes consumer
	// back-pressure. Always zero on HTTP, where the profile travels in a
	// response header that must be flushed before the body.
	DeliverDuration time.Duration
	// FirstRowDuration is handler entry to the first row accepted by the
	// transport. Zero means "no row was ever handed to the transport": an empty
	// result, a unary response, or a failure before the first send. Read it
	// together with ItemsCollected to tell those apart.
	FirstRowDuration time.Duration
	// ServerDuration is the consumer-independent server cost. Computed by
	// Finish; zero until then.
	ServerDuration time.Duration
	// Forwarded is true when the read was routed to another node (an explicit
	// leader read, or the syncing-follower fallback). The remote node runs its
	// own barrier and execution, and that whole cost lands in this profile's
	// ExecuteDuration.
	//
	// Forwarded does NOT imply BarrierDuration == 0, and a non-zero value does
	// not identify which wait occurred. Two paths produce one: the
	// syncing-follower fallback attempts a ReadIndex barrier before forwarding,
	// and waitMinLogSequence charges its catch-up regardless of consistency
	// level, so an explicit leader read with min_log_sequence set reports a wait
	// on a healthy cluster. The flag's job is narrower: stop a zero from being
	// misread as "no barrier was needed".
	Forwarded bool
	// Anomaly is non-empty when the phase bookkeeping detected a state that is
	// impossible by contract (see clampPhase). It is surfaced in the log and on
	// the span so the violation cannot pass unnoticed, per invariant #7.
	Anomaly string

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
// validation and filter compilation — otherwise those phases stay outside the
// measured window and PrepareDuration is meaningless. Where authentication runs
// above the handler, as it does in the HTTP router, use
// [WithProfileStartingAt] with an instant stamped before it instead.
//
// The profile is collected in full for every profiled read, whether or not the
// caller asked to see it: the slow-query log and the OTel span consume the same
// record, and a measurement regime that depended on the caller having asked
// would make the two regimes incomparable — the exact defect this timing work
// exists to remove. The presence of a profile in the context, not the caller's
// request for one, is what enables instrumentation.
func WithProfile(ctx context.Context) (context.Context, *QueryProfile) {
	return WithProfileStartingAt(ctx, time.Now())
}

// WithProfileStartingAt is [WithProfile] with an externally captured start
// instant, for transports where the first instrumentable point sits above the
// handler.
//
// HTTP needs it: authentication runs in a router-wide middleware, so a profile
// created in the handler would exclude it, while the gRPC handlers call
// WithProfile before internalauth.Authenticate and include it. Same field name,
// different span — so the HTTP router stamps the instant just before
// authenticating and hands it here.
func WithProfileStartingAt(ctx context.Context, start time.Time) (context.Context, *QueryProfile) {
	p := &QueryProfile{requestStart: start}

	return context.WithValue(ctx, profileKey{}, p), p
}

// MarkForwarded records that the read was served by another node, so a zero
// BarrierDuration is not misread as "no barrier was needed". Nil-safe.
func (p *QueryProfile) MarkForwarded() {
	if p == nil {
		return
	}

	p.Forwarded = true
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
		p.PrepareDuration = p.clampPhase("prepare", time.Since(p.requestStart)-p.BarrierDuration)
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

	p.ExecuteDuration += p.clampPhase("execute", time.Since(p.executeStart)-(p.BarrierDuration-p.barrierAtExecuteStart))
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

	p.ServerDuration = p.clampPhase("server", time.Since(p.requestStart)-p.BarrierDuration-p.DeliverDuration)

	if !p.executeEntered {
		// The request never reached the executor (validation rejection, missing
		// ledger, …): everything measured was preparation.
		p.PrepareDuration = p.ServerDuration
	}
}

// clampPhase floors a computed phase duration at zero.
//
// A negative result is impossible by contract: every accumulator is credited
// from inside the window it is later subtracted from, so it can only exceed that
// window if a hook double-counted. Per invariant #7 that must not pass silently,
// but a read request is the wrong place to fail hard over a diagnostic — so the
// clamp keeps the wire value sane while the violation is recorded on the profile
// and asserted for the fuzzing harness.
//
// Reaching an operator is best-effort, not guaranteed: the profile is logged only
// above the slow-query threshold, and clamping ServerDuration to zero is exactly
// what can pull the compared total below it. The fuzzing assertion is the
// reliable detector; the recorded string is what makes it legible if a real
// deployment ever does log one.
func (p *QueryProfile) clampPhase(phase string, d time.Duration) time.Duration {
	if d >= 0 {
		return d
	}

	assert.Unreachable("query profile: phase duration went negative — an accumulator was double-counted", map[string]any{
		"phase":             phase,
		"duration_ns":       int64(d),
		"barrier_ns":        int64(p.BarrierDuration),
		"deliver_ns":        int64(p.DeliverDuration),
		"execute_ns":        int64(p.ExecuteDuration),
		"executeEntered":    p.executeEntered,
		"executeWindowOpen": !p.executeStart.IsZero(),
	})

	// assert.Unreachable is a no-op in production builds, so leave a trace the
	// operator can actually see.
	p.Anomaly = "negative " + phase + " duration (accumulator double-counted)"

	return 0
}

// ProfileFromContext extracts the QueryProfile from the context.
// Returns nil if no profile was set.
func ProfileFromContext(ctx context.Context) *QueryProfile {
	p, _ := ctx.Value(profileKey{}).(*QueryProfile)

	return p
}

// TotalDuration returns the sum of index and enrichment durations, i.e. the
// query EXECUTION total. It is a subset of ServerDuration and must not be used
// as "how long the server took" — use ServerDuration or WallDuration for that.
func (p *QueryProfile) TotalDuration() time.Duration {
	return p.IndexDuration + p.EnrichmentDuration
}

// WallDuration returns everything the server spent on the request except the
// caller-requested read barrier: ServerDuration plus the delivery phase.
//
// This is the total the slow-query threshold compares against. ServerDuration
// alone would be blind to two real costs: row serialisation, which grows with
// page size and is charged to delivery because a gRPC stream.Send() marshals and
// writes in one inseparable call; and, on a forwarded read, the remote node's
// entire cost, which arrives through the row-production side of the send loop.
// A threshold that cannot see either would fail to surface exactly the slow
// requests it exists for.
//
// The price is that on a server stream this total also absorbs consumer
// back-pressure, so a slow client can trip the threshold. That is accepted
// deliberately: the logged breakdown (DeliverDuration vs FirstRowDuration vs
// ServerDuration) tells the reader which side was slow, whereas a threshold that
// never fires tells them nothing at all.
func (p *QueryProfile) WallDuration() time.Duration {
	if p == nil {
		return 0
	}

	return p.ServerDuration + p.DeliverDuration
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
		Forwarded:            p.Forwarded,
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
		attribute.Int64("query.wall_duration_us", p.WallDuration().Microseconds()),
		attribute.Bool("query.forwarded", p.Forwarded),
	)

	if p.Anomaly != "" {
		span.SetAttributes(attribute.String("query.profile_anomaly", p.Anomaly))
	}

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
			"wallDurationUs":       p.WallDuration().Microseconds(),
			// True means the read was served by another node, so the remote node's
			// whole cost sits inside executeDurationUs. barrierDurationUs then
			// covers the local attempt only: 0 for an explicit leader read, non-zero
			// when a local barrier failed before the fallback.
			"forwarded": p.Forwarded,
		}
		if p.Root != nil {
			fields["iteratorTree"] = p.Root.String()
		}

		if p.Anomaly != "" {
			fields["profileAnomaly"] = p.Anomaly
		}

		logger.WithFields(fields).Tracef("Query profile (wall=%s, server=%s, execution=%s)",
			p.WallDuration(), p.ServerDuration, p.TotalDuration())
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
