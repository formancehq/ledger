package state

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/signal"
	"google.golang.org/protobuf/proto"
)

// PeriodTracker encapsulates all period-related state: the map of all
// non-purged periods, convenience pointers to the open/closing periods,
// the auto-incrementing period ID, and the cron schedule.
type PeriodTracker struct {
	allPeriods        map[uint64]*commonpb.Period
	currentOpenPeriod *commonpb.Period
	closingPeriod     *commonpb.Period
	nextPeriodID      uint64

	// schedule is the cron expression for automatic period rotation (empty = disabled).
	schedule string
	// scheduleChanged fires when the schedule is updated via Raft.
	scheduleChanged signal.Signal
}

// NewPeriodTracker creates a PeriodTracker from persisted state loaded at startup.
func NewPeriodTracker(
	allPeriods map[uint64]*commonpb.Period,
	currentOpenPeriod *commonpb.Period,
	closingPeriod *commonpb.Period,
	nextPeriodID uint64,
	schedule string,
) *PeriodTracker {
	return &PeriodTracker{
		allPeriods:        allPeriods,
		currentOpenPeriod: currentOpenPeriod,
		closingPeriod:     closingPeriod,
		nextPeriodID:      nextPeriodID,
		schedule:          schedule,
		scheduleChanged:   signal.New(),
	}
}

// AllPeriods returns a slice of all non-purged periods.
func (pt *PeriodTracker) AllPeriods() []*commonpb.Period {
	periods := make([]*commonpb.Period, 0, len(pt.allPeriods))
	for _, p := range pt.allPeriods {
		periods = append(periods, p)
	}
	return periods
}

// CurrentOpenPeriod returns the period currently in OPEN state, or nil.
func (pt *PeriodTracker) CurrentOpenPeriod() *commonpb.Period {
	return pt.currentOpenPeriod
}

// SetCurrentOpenPeriod sets the current open period.
func (pt *PeriodTracker) SetCurrentOpenPeriod(p *commonpb.Period) {
	pt.currentOpenPeriod = p
	if p != nil {
		pt.allPeriods[p.Id] = p
	}
}

// ClosingPeriod returns the period currently in CLOSING state, or nil.
func (pt *PeriodTracker) ClosingPeriod() *commonpb.Period {
	return pt.closingPeriod
}

// SetClosingPeriod sets the closing period.
func (pt *PeriodTracker) SetClosingPeriod(p *commonpb.Period) {
	pt.closingPeriod = p
}

// ClearClosingPeriod removes the closing period reference.
func (pt *PeriodTracker) ClearClosingPeriod() {
	pt.closingPeriod = nil
}

// GetPeriodByID looks up a period by ID.
func (pt *PeriodTracker) GetPeriodByID(id uint64) (*commonpb.Period, bool) {
	p, ok := pt.allPeriods[id]
	return p, ok
}

// PutPeriod adds or updates a period in the map.
func (pt *PeriodTracker) PutPeriod(p *commonpb.Period) {
	pt.allPeriods[p.Id] = p
}

// DeletePeriod removes a period from the map.
func (pt *PeriodTracker) DeletePeriod(id uint64) {
	delete(pt.allPeriods, id)
}

// NextPeriodID returns the next period ID to assign.
func (pt *PeriodTracker) NextPeriodID() uint64 {
	return pt.nextPeriodID
}

// SetNextPeriodID sets the next period ID.
func (pt *PeriodTracker) SetNextPeriodID(id uint64) {
	pt.nextPeriodID = id
}

// Schedule returns the cron expression for automatic period rotation.
func (pt *PeriodTracker) Schedule() string {
	return pt.schedule
}

// SetSchedule updates the schedule and fires the changed signal.
func (pt *PeriodTracker) SetSchedule(s string) {
	pt.schedule = s
	pt.scheduleChanged.Notify()
}

// ScheduleChanged returns the Signal that fires when the schedule is updated.
func (pt *PeriodTracker) ScheduleChanged() signal.Signal {
	return pt.scheduleChanged
}

// Reset rebuilds the tracker from a snapshot's period data.
func (pt *PeriodTracker) Reset(
	allPeriods map[uint64]*commonpb.Period,
	currentOpenPeriod *commonpb.Period,
	closingPeriod *commonpb.Period,
	nextPeriodID uint64,
) {
	pt.allPeriods = allPeriods
	pt.currentOpenPeriod = currentOpenPeriod
	pt.closingPeriod = closingPeriod
	pt.nextPeriodID = nextPeriodID
}

// Clone returns a deep copy of the PeriodTracker suitable for use in a Buffer.
// The schedule and scheduleChanged signal are NOT cloned — they are Machine-level
// concerns that the Buffer never reads or writes.
func (pt *PeriodTracker) Clone() *PeriodTracker {
	clonedAll := make(map[uint64]*commonpb.Period, len(pt.allPeriods))
	for id, p := range pt.allPeriods {
		clonedAll[id] = proto.CloneOf(p)
	}

	var clonedOpen, clonedClosing *commonpb.Period
	if pt.currentOpenPeriod != nil {
		clonedOpen = clonedAll[pt.currentOpenPeriod.Id]
	}
	if pt.closingPeriod != nil {
		clonedClosing = clonedAll[pt.closingPeriod.Id]
	}

	return &PeriodTracker{
		allPeriods:        clonedAll,
		currentOpenPeriod: clonedOpen,
		closingPeriod:     clonedClosing,
		nextPeriodID:      pt.nextPeriodID,
	}
}
