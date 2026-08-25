package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/query"
)

// The tests below assert the *shape* of the phase model (which window charges
// which accumulator, what is excluded from the total) rather than absolute
// wall-clock values. Where an exact number is needed, it comes from an
// accumulator the test feeds itself (AddBarrierWait / AddDelivery /
// AddProduction) — never from sleeping, which would make the suite slow and
// flaky without proving anything the inequalities below do not.

func TestQueryProfile_PhaseDecomposition(t *testing.T) {
	t.Parallel()

	_, p := query.WithProfile(context.Background())

	p.EnterExecute()
	p.LeaveExecute()
	p.Finish()

	assert.Positive(t, p.ServerDuration, "the request clock must have advanced")
	assert.GreaterOrEqual(t, p.ServerDuration, p.PrepareDuration+p.ExecuteDuration,
		"prepare + execute must fit inside the server total; the difference is unattributed server work")
}

func TestQueryProfile_BarrierExcludedFromServerDuration(t *testing.T) {
	t.Parallel()

	_, p := query.WithProfile(context.Background())

	p.EnterExecute()
	p.LeaveExecute()
	// A barrier wait far larger than anything the test itself spent: if it were
	// counted as server cost the total would be dominated by it.
	p.AddBarrierWait(time.Hour)
	p.Finish()

	assert.Equal(t, time.Hour, p.BarrierDuration, "the wait must still be reported")
	assert.Zero(t, p.ServerDuration, "but never charged to the server total")
	assert.Zero(t, p.WallDuration(), "nor to the wall total the slow-query threshold uses")
	assert.NotEmpty(t, p.Anomaly, "a phase driven negative must be reported, not silently clamped")
}

func TestQueryProfile_DeliveryExcludedFromServerDuration(t *testing.T) {
	t.Parallel()

	_, p := query.WithProfile(context.Background())

	p.EnterExecute()
	p.LeaveExecute()
	// Stands in for a consumer that stopped draining the stream: the server
	// total must not move with it.
	p.AddDelivery(time.Hour)
	p.Finish()

	assert.Equal(t, time.Hour, p.DeliverDuration)
	assert.Zero(t, p.ServerDuration)

	// But the slow-query threshold must still see it, otherwise serialisation
	// cost and forwarded-read cost could hide from the log.
	assert.GreaterOrEqual(t, p.WallDuration(), time.Hour)
}

func TestQueryProfile_WallDurationIncludesDelivery(t *testing.T) {
	t.Parallel()

	p := &query.QueryProfile{
		ServerDuration:  10 * time.Millisecond,
		DeliverDuration: 4 * time.Millisecond,
	}

	assert.Equal(t, 14*time.Millisecond, p.WallDuration())
}

func TestQueryProfile_WallDuration_NilSafe(t *testing.T) {
	t.Parallel()

	var p *query.QueryProfile
	assert.Zero(t, p.WallDuration())
}

func TestQueryProfile_MarkForwarded(t *testing.T) {
	t.Parallel()

	_, p := query.WithProfile(context.Background())
	assert.False(t, p.Forwarded)

	p.MarkForwarded()
	assert.True(t, p.Forwarded,
		"a forwarded read must be flagged so its zero BarrierDuration is not read as 'no barrier needed'")
}

// Forwarded does NOT imply a zero barrier. RoutedController.readCtrl attempts a
// local ReadIndex barrier first and only forwards when it fails, so the failed
// attempt is already recorded when MarkForwarded runs. That combination is the
// documented contract, not a bookkeeping error: the caller really waited, and a
// failed quorum wait is no more server work than a successful one. Zeroing it
// would push consensus latency into PrepareDuration and into the server total.
func TestQueryProfile_ForwardedKeepsFailedLocalBarrier(t *testing.T) {
	t.Parallel()

	_, p := query.WithProfile(context.Background())

	// The syncing-follower fallback, in order: barrier attempted, barrier fails,
	// read forwarded, remote cost arrives as row production.
	p.AddBarrierWait(time.Hour)
	p.MarkForwarded()
	p.EnterExecute()
	p.LeaveExecute()
	p.Finish()

	assert.True(t, p.Forwarded)
	assert.Equal(t, time.Hour, p.BarrierDuration,
		"the failed local attempt must survive MarkForwarded — dropping it hides real caller-visible latency")
	assert.Zero(t, p.ServerDuration, "and stay excluded from the server total, exactly like a successful barrier")
	assert.Zero(t, p.PrepareDuration, "in particular it must not leak into prepare, which means request setup work")
}

func TestQueryProfile_BarrierInsideExecuteNotChargedToExecute(t *testing.T) {
	t.Parallel()

	_, p := query.WithProfile(context.Background())

	p.EnterExecute()
	// A ReadIndex quorum round-trip observed from inside the executor call.
	p.AddBarrierWait(time.Hour)
	p.LeaveExecute()

	assert.Zero(t, p.ExecuteDuration, "consensus wait is not execution work")
	assert.Equal(t, time.Hour, p.BarrierDuration)
}

func TestQueryProfile_BarrierBeforeExecuteNotChargedToPrepare(t *testing.T) {
	t.Parallel()

	_, p := query.WithProfile(context.Background())

	p.AddBarrierWait(time.Hour)
	p.EnterExecute()

	assert.Zero(t, p.PrepareDuration, "min_log_sequence wait is not preparation work")
}

func TestQueryProfile_AddProductionChargedToExecute(t *testing.T) {
	t.Parallel()

	_, p := query.WithProfile(context.Background())

	p.EnterExecute()
	p.LeaveExecute()

	before := p.ExecuteDuration
	// Rows pulled lazily from a routed upstream stream inside the send loop.
	p.AddProduction(5 * time.Millisecond)

	assert.Equal(t, before+5*time.Millisecond, p.ExecuteDuration)
}

func TestQueryProfile_AbortedBeforeExecuteIsAllPrepare(t *testing.T) {
	t.Parallel()

	_, p := query.WithProfile(context.Background())

	// Request rejected during validation: it never reached the executor.
	p.Finish()

	assert.Zero(t, p.ExecuteDuration)
	assert.Equal(t, p.ServerDuration, p.PrepareDuration)
}

func TestQueryProfile_FinishClosesOpenExecuteWindow(t *testing.T) {
	t.Parallel()

	_, p := query.WithProfile(context.Background())

	p.EnterExecute()
	// No LeaveExecute: an early return inside the handler.
	p.Finish()

	assert.LessOrEqual(t, p.ExecuteDuration, p.ServerDuration)
	assert.GreaterOrEqual(t, p.ExecuteDuration, time.Duration(0))
}

func TestQueryProfile_FinishIsIdempotent(t *testing.T) {
	t.Parallel()

	_, p := query.WithProfile(context.Background())

	p.EnterExecute()
	p.LeaveExecute()
	p.Finish()

	first := p.ServerDuration
	p.Finish()

	assert.Equal(t, first, p.ServerDuration, "a second Finish must not extend the total")
}

func TestQueryProfile_MarkFirstRowRecordsOnlyTheFirst(t *testing.T) {
	t.Parallel()

	_, p := query.WithProfile(context.Background())

	p.MarkFirstRow()
	first := p.FirstRowDuration
	require.Positive(t, first)

	p.MarkFirstRow()
	assert.Equal(t, first, p.FirstRowDuration)
}

func TestQueryProfile_HandBuiltProfileKeepsAssignedDurations(t *testing.T) {
	t.Parallel()

	// A profile built by hand (tests, fixtures) has no request clock, so the
	// phase marks must leave the values it was given alone rather than
	// overwriting them with meaningless measurements.
	p := &query.QueryProfile{
		ServerDuration:  9 * time.Millisecond,
		PrepareDuration: 4 * time.Millisecond,
		ExecuteDuration: 5 * time.Millisecond,
	}

	p.EnterExecute()
	p.LeaveExecute()
	p.MarkFirstRow()
	p.Finish()

	assert.Equal(t, 9*time.Millisecond, p.ServerDuration)
	assert.Equal(t, 4*time.Millisecond, p.PrepareDuration)
	assert.Equal(t, 5*time.Millisecond, p.ExecuteDuration)
	assert.Zero(t, p.FirstRowDuration)
}

func TestQueryProfile_TimingNilSafe(t *testing.T) {
	t.Parallel()

	var p *query.QueryProfile

	// Every hook is called from code paths that may run without a profile
	// (unprofiled RPCs share the same helpers), so none may panic.
	p.EnterExecute()
	p.LeaveExecute()
	p.AddBarrierWait(time.Second)
	p.AddProduction(time.Second)
	p.AddDelivery(time.Second)
	p.MarkFirstRow()
	p.Finish()
}

func TestQueryProfile_ToProto_ServerTiming(t *testing.T) {
	t.Parallel()

	p := &query.QueryProfile{
		ServerDuration:   56 * time.Millisecond,
		PrepareDuration:  40 * time.Millisecond,
		ExecuteDuration:  14 * time.Millisecond,
		BarrierDuration:  3 * time.Millisecond,
		DeliverDuration:  7 * time.Millisecond,
		FirstRowDuration: 55 * time.Millisecond,
	}

	pb := p.ToProto()
	require.NotNil(t, pb)
	assert.Equal(t, int64(56000), pb.GetServerDurationUs())
	assert.Equal(t, int64(40000), pb.GetPrepareDurationUs())
	assert.Equal(t, int64(14000), pb.GetExecuteDurationUs())
	assert.Equal(t, int64(3000), pb.GetBarrierDurationUs())
	assert.Equal(t, int64(7000), pb.GetDeliverDurationUs())
	assert.Equal(t, int64(55000), pb.GetFirstRowDurationUs())
}
