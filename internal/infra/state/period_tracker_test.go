package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

func newTestPeriodTracker() *PeriodTracker {
	return NewPeriodTracker(
		make(map[uint64]*commonpb.Period),
		nil, nil, 1, "",
	)
}

func TestPeriodTrackerNewAndDefaults(t *testing.T) {
	t.Parallel()

	pt := newTestPeriodTracker()

	require.Empty(t, pt.AllPeriods())
	require.Nil(t, pt.CurrentOpenPeriod())
	require.Empty(t, pt.ClosingPeriods())
	require.Equal(t, uint64(1), pt.NextPeriodID())
	require.Empty(t, pt.Schedule())
	require.NotNil(t, pt.ScheduleChanged())
}

func TestPeriodTrackerNewWithState(t *testing.T) {
	t.Parallel()

	open := &commonpb.Period{Id: 1, Status: commonpb.PeriodStatus_PERIOD_OPEN}
	closing := &commonpb.Period{Id: 2, Status: commonpb.PeriodStatus_PERIOD_CLOSING}
	all := map[uint64]*commonpb.Period{1: open, 2: closing}

	pt := NewPeriodTracker(all, open, []*commonpb.Period{closing}, 3, "0 * * * *")

	require.Len(t, pt.AllPeriods(), 2)
	require.Equal(t, open, pt.CurrentOpenPeriod())
	require.Len(t, pt.ClosingPeriods(), 1)
	require.Equal(t, closing, pt.ClosingPeriods()[0])
	require.Equal(t, uint64(3), pt.NextPeriodID())
	require.Equal(t, "0 * * * *", pt.Schedule())
}

func TestPeriodTrackerPutAndGet(t *testing.T) {
	t.Parallel()

	pt := newTestPeriodTracker()

	_, ok := pt.GetPeriodByID(10)
	require.False(t, ok)

	p := &commonpb.Period{Id: 10, Status: commonpb.PeriodStatus_PERIOD_CLOSED}
	pt.PutPeriod(p)

	got, ok := pt.GetPeriodByID(10)
	require.True(t, ok)
	require.Equal(t, p, got)
	require.Len(t, pt.AllPeriods(), 1)
}

func TestPeriodTrackerDeletePeriod(t *testing.T) {
	t.Parallel()

	pt := newTestPeriodTracker()
	pt.PutPeriod(&commonpb.Period{Id: 5})
	pt.PutPeriod(&commonpb.Period{Id: 6})
	require.Len(t, pt.AllPeriods(), 2)

	pt.DeletePeriod(5)

	_, ok := pt.GetPeriodByID(5)
	require.False(t, ok)
	require.Len(t, pt.AllPeriods(), 1)
}

func TestPeriodTrackerSetCurrentOpenPeriod(t *testing.T) {
	t.Parallel()

	pt := newTestPeriodTracker()
	require.Nil(t, pt.CurrentOpenPeriod())

	p := &commonpb.Period{Id: 1, Status: commonpb.PeriodStatus_PERIOD_OPEN}
	pt.SetCurrentOpenPeriod(p)

	require.Equal(t, p, pt.CurrentOpenPeriod())
	// SetCurrentOpenPeriod also adds to allPeriods
	got, ok := pt.GetPeriodByID(1)
	require.True(t, ok)
	require.Equal(t, p, got)

	// Setting nil clears the open period but does not add to allPeriods
	pt.SetCurrentOpenPeriod(nil)
	require.Nil(t, pt.CurrentOpenPeriod())
	// period 1 should still be in allPeriods
	_, ok = pt.GetPeriodByID(1)
	require.True(t, ok)
}

func TestPeriodTrackerClosingPeriodsLifecycle(t *testing.T) {
	t.Parallel()

	pt := newTestPeriodTracker()
	require.Empty(t, pt.ClosingPeriods())

	p1 := &commonpb.Period{Id: 3, Status: commonpb.PeriodStatus_PERIOD_CLOSING}
	pt.AddClosingPeriod(p1)
	require.Len(t, pt.ClosingPeriods(), 1)

	cp, ok := pt.ClosingPeriodByID(3)
	require.True(t, ok)
	require.Equal(t, p1, cp)

	// LatestClosingPeriod returns the last added
	require.Equal(t, p1, pt.LatestClosingPeriod())

	// Add a second
	p2 := &commonpb.Period{Id: 4, Status: commonpb.PeriodStatus_PERIOD_CLOSING}
	pt.AddClosingPeriod(p2)
	require.Len(t, pt.ClosingPeriods(), 2)
	require.Equal(t, p2, pt.LatestClosingPeriod())

	// Remove first by ID
	pt.RemoveClosingPeriod(3)
	require.Len(t, pt.ClosingPeriods(), 1)
	_, ok = pt.ClosingPeriodByID(3)
	require.False(t, ok)

	// Remove second
	pt.RemoveClosingPeriod(4)
	require.Empty(t, pt.ClosingPeriods())
	require.Nil(t, pt.LatestClosingPeriod())
}

func TestPeriodTrackerRemoveClosingPeriodNonExistent(t *testing.T) {
	t.Parallel()

	pt := newTestPeriodTracker()
	p := &commonpb.Period{Id: 1, Status: commonpb.PeriodStatus_PERIOD_CLOSING}
	pt.AddClosingPeriod(p)

	// Removing a non-existent ID is a no-op
	pt.RemoveClosingPeriod(999)
	require.Len(t, pt.ClosingPeriods(), 1)
}

func TestPeriodTrackerClosingPeriodByIDNotFound(t *testing.T) {
	t.Parallel()

	pt := newTestPeriodTracker()
	pt.AddClosingPeriod(&commonpb.Period{Id: 5})

	_, ok := pt.ClosingPeriodByID(99)
	require.False(t, ok)
}

func TestPeriodTrackerSetClosingPeriodsBulk(t *testing.T) {
	t.Parallel()

	pt := newTestPeriodTracker()
	pt.AddClosingPeriod(&commonpb.Period{Id: 1})

	// Bulk replace
	newPeriods := []*commonpb.Period{
		{Id: 10, Status: commonpb.PeriodStatus_PERIOD_CLOSING},
		{Id: 11, Status: commonpb.PeriodStatus_PERIOD_CLOSING},
	}
	pt.SetClosingPeriods(newPeriods)

	require.Len(t, pt.ClosingPeriods(), 2)
	_, ok := pt.ClosingPeriodByID(1)
	require.False(t, ok)
	_, ok = pt.ClosingPeriodByID(10)
	require.True(t, ok)
	_, ok = pt.ClosingPeriodByID(11)
	require.True(t, ok)
}

func TestPeriodTrackerCloneMultipleClosingPeriods(t *testing.T) {
	t.Parallel()

	p1 := &commonpb.Period{Id: 1, Status: commonpb.PeriodStatus_PERIOD_CLOSING, CloseSequence: 10}
	p2 := &commonpb.Period{Id: 2, Status: commonpb.PeriodStatus_PERIOD_CLOSING, CloseSequence: 20}
	all := map[uint64]*commonpb.Period{1: p1, 2: p2}

	pt := NewPeriodTracker(all, nil, []*commonpb.Period{p1, p2}, 5, "")
	clone := pt.Clone()

	require.Len(t, clone.ClosingPeriods(), 2)
	require.Equal(t, uint64(1), clone.ClosingPeriods()[0].GetId())
	require.Equal(t, uint64(2), clone.ClosingPeriods()[1].GetId())

	// Mutating clone doesn't affect original
	clone.ClosingPeriods()[0].CloseSequence = 999
	require.Equal(t, uint64(10), pt.ClosingPeriods()[0].GetCloseSequence())

	clone.RemoveClosingPeriod(1)
	require.Len(t, clone.ClosingPeriods(), 1)
	require.Len(t, pt.ClosingPeriods(), 2)
}

func TestPeriodTrackerResetMultipleClosingPeriods(t *testing.T) {
	t.Parallel()

	pt := newTestPeriodTracker()

	c1 := &commonpb.Period{Id: 10, Status: commonpb.PeriodStatus_PERIOD_CLOSING}
	c2 := &commonpb.Period{Id: 11, Status: commonpb.PeriodStatus_PERIOD_CLOSING}
	newAll := map[uint64]*commonpb.Period{10: c1, 11: c2}

	pt.Reset(newAll, nil, []*commonpb.Period{c1, c2}, 20)

	require.Len(t, pt.ClosingPeriods(), 2)
	require.Equal(t, c2, pt.LatestClosingPeriod())
}

func TestPeriodTrackerNextPeriodID(t *testing.T) {
	t.Parallel()

	pt := newTestPeriodTracker()
	require.Equal(t, uint64(1), pt.NextPeriodID())

	pt.SetNextPeriodID(42)
	require.Equal(t, uint64(42), pt.NextPeriodID())
}

func TestPeriodTrackerSchedule(t *testing.T) {
	t.Parallel()

	pt := newTestPeriodTracker()
	require.Empty(t, pt.Schedule())

	sig := pt.ScheduleChanged()

	pt.SetSchedule("*/5 * * * *")
	require.Equal(t, "*/5 * * * *", pt.Schedule())

	// Signal should have been notified
	select {
	case <-sig.C():
		// ok
	default:
		t.Fatal("expected schedule changed signal to fire")
	}

	// Clear schedule
	pt.SetSchedule("")
	require.Empty(t, pt.Schedule())
}

func TestPeriodTrackerReset(t *testing.T) {
	t.Parallel()

	pt := newTestPeriodTracker()
	pt.PutPeriod(&commonpb.Period{Id: 1})
	pt.SetSchedule("@daily")
	require.Equal(t, "@daily", pt.Schedule())

	newOpen := &commonpb.Period{Id: 10, Status: commonpb.PeriodStatus_PERIOD_OPEN}
	newClosing := &commonpb.Period{Id: 11, Status: commonpb.PeriodStatus_PERIOD_CLOSING}
	newAll := map[uint64]*commonpb.Period{10: newOpen, 11: newClosing}

	pt.Reset(newAll, newOpen, []*commonpb.Period{newClosing}, 12)

	require.Len(t, pt.AllPeriods(), 2)
	require.Equal(t, newOpen, pt.CurrentOpenPeriod())
	require.Len(t, pt.ClosingPeriods(), 1)
	require.Equal(t, newClosing, pt.ClosingPeriods()[0])
	require.Equal(t, uint64(12), pt.NextPeriodID())
	// Schedule is preserved across Reset (Machine-level concern)
	require.Equal(t, "@daily", pt.Schedule())
	// Old period is gone
	_, ok := pt.GetPeriodByID(1)
	require.False(t, ok)
}

func TestPeriodTrackerClone(t *testing.T) {
	t.Parallel()

	open := &commonpb.Period{Id: 1, Status: commonpb.PeriodStatus_PERIOD_OPEN, StartSequence: 10}
	closing := &commonpb.Period{Id: 2, Status: commonpb.PeriodStatus_PERIOD_CLOSING, CloseSequence: 20}
	closed := &commonpb.Period{Id: 3, Status: commonpb.PeriodStatus_PERIOD_CLOSED}
	all := map[uint64]*commonpb.Period{1: open, 2: closing, 3: closed}

	pt := NewPeriodTracker(all, open, []*commonpb.Period{closing}, 4, "*/10 * * * *")
	clone := pt.Clone()

	// Clone has same data
	require.Len(t, clone.AllPeriods(), 3)
	require.Equal(t, uint64(4), clone.NextPeriodID())
	require.NotNil(t, clone.CurrentOpenPeriod())
	require.Equal(t, uint64(1), clone.CurrentOpenPeriod().GetId())
	require.Len(t, clone.ClosingPeriods(), 1)
	require.Equal(t, uint64(2), clone.ClosingPeriods()[0].GetId())

	// Clone is a deep copy — mutating clone doesn't affect original
	clone.CurrentOpenPeriod().StartSequence = 999
	require.Equal(t, uint64(10), pt.CurrentOpenPeriod().GetStartSequence())

	clone.PutPeriod(&commonpb.Period{Id: 100})
	require.Len(t, clone.AllPeriods(), 4)
	require.Len(t, pt.AllPeriods(), 3)

	clone.SetNextPeriodID(99)
	require.Equal(t, uint64(4), pt.NextPeriodID())

	// Schedule is NOT cloned
	require.Empty(t, clone.Schedule())
}

func TestPeriodTrackerCloneNilPeriods(t *testing.T) {
	t.Parallel()

	pt := newTestPeriodTracker()
	pt.PutPeriod(&commonpb.Period{Id: 5, Status: commonpb.PeriodStatus_PERIOD_CLOSED})

	clone := pt.Clone()

	require.Nil(t, clone.CurrentOpenPeriod())
	require.Empty(t, clone.ClosingPeriods())
	require.Len(t, clone.AllPeriods(), 1)
}
