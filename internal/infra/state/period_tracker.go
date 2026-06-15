package state

import (
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// PeriodTracker encapsulates all period-related state: the map of all
// non-purged periods, convenience pointers to the open/closing periods,
// the auto-incrementing period ID, and the cron schedule.
type PeriodTracker struct {
	mu                sync.RWMutex
	allPeriods        map[uint64]*commonpb.Period
	currentOpenPeriod *commonpb.Period
	closingPeriods    []*commonpb.Period
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
	closingPeriods []*commonpb.Period,
	nextPeriodID uint64,
	schedule string,
) *PeriodTracker {
	if allPeriods == nil {
		allPeriods = map[uint64]*commonpb.Period{}
	}

	return &PeriodTracker{
		allPeriods:        allPeriods,
		currentOpenPeriod: currentOpenPeriod,
		closingPeriods:    closingPeriods,
		nextPeriodID:      nextPeriodID,
		schedule:          schedule,
		scheduleChanged:   signal.New(),
	}
}

// AllPeriods returns a slice of all non-purged periods.
func (pt *PeriodTracker) AllPeriods() []*commonpb.Period {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	periods := make([]*commonpb.Period, 0, len(pt.allPeriods))
	for _, p := range pt.allPeriods {
		periods = append(periods, p)
	}

	return periods
}

// CurrentOpenPeriod returns the period currently in OPEN state, or nil.
func (pt *PeriodTracker) CurrentOpenPeriod() *commonpb.Period {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return pt.currentOpenPeriod
}

// SetCurrentOpenPeriod sets the current open period.
func (pt *PeriodTracker) SetCurrentOpenPeriod(p *commonpb.Period) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.currentOpenPeriod = p
	if p != nil {
		pt.allPeriods[p.GetId()] = p
	}
}

// ClosingPeriods returns all periods currently in CLOSING state.
func (pt *PeriodTracker) ClosingPeriods() []*commonpb.Period {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return pt.closingPeriods
}

// ClosingPeriodByID returns the closing period with the given ID, if any.
func (pt *PeriodTracker) ClosingPeriodByID(id uint64) (*commonpb.Period, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	for _, p := range pt.closingPeriods {
		if p.GetId() == id {
			return p, true
		}
	}

	return nil, false
}

// LatestClosingPeriod returns the most recently added closing period, or nil.
func (pt *PeriodTracker) LatestClosingPeriod() *commonpb.Period {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	if len(pt.closingPeriods) == 0 {
		return nil
	}

	return pt.closingPeriods[len(pt.closingPeriods)-1]
}

// AddClosingPeriod appends a period to the closing periods list.
func (pt *PeriodTracker) AddClosingPeriod(p *commonpb.Period) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.closingPeriods = append(pt.closingPeriods, p)
}

// RemoveClosingPeriod removes the closing period with the given ID.
func (pt *PeriodTracker) RemoveClosingPeriod(id uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	for i, p := range pt.closingPeriods {
		if p.GetId() == id {
			pt.closingPeriods = append(pt.closingPeriods[:i], pt.closingPeriods[i+1:]...)

			return
		}
	}
}

// SetClosingPeriods replaces the entire closing periods slice (used during Commit).
func (pt *PeriodTracker) SetClosingPeriods(periods []*commonpb.Period) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.closingPeriods = periods
}

// GetPeriodByID looks up a period by ID.
func (pt *PeriodTracker) GetPeriodByID(id uint64) (*commonpb.Period, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	p, ok := pt.allPeriods[id]

	return p, ok
}

// PutPeriod adds or updates a period in the map.
func (pt *PeriodTracker) PutPeriod(p *commonpb.Period) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.allPeriods[p.GetId()] = p
}

// DeletePeriod removes a period from the map.
func (pt *PeriodTracker) DeletePeriod(id uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	delete(pt.allPeriods, id)
}

// NextPeriodID returns the next period ID to assign.
func (pt *PeriodTracker) NextPeriodID() uint64 {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return pt.nextPeriodID
}

// SetNextPeriodID sets the next period ID.
func (pt *PeriodTracker) SetNextPeriodID(id uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.nextPeriodID = id
}

// Schedule returns the cron expression for automatic period rotation.
func (pt *PeriodTracker) Schedule() string {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return pt.schedule
}

// SetSchedule updates the schedule and fires the changed signal.
func (pt *PeriodTracker) SetSchedule(s string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.schedule = s
	pt.scheduleChanged.Notify()
}

// ScheduleChanged returns the Signal that fires when the schedule is updated.
func (pt *PeriodTracker) ScheduleChanged() signal.Signal {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return pt.scheduleChanged
}

// Reset rebuilds the tracker from a snapshot's period data.
func (pt *PeriodTracker) Reset(
	allPeriods map[uint64]*commonpb.Period,
	currentOpenPeriod *commonpb.Period,
	closingPeriods []*commonpb.Period,
	nextPeriodID uint64,
) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.allPeriods = allPeriods
	pt.currentOpenPeriod = currentOpenPeriod
	pt.closingPeriods = closingPeriods
	pt.nextPeriodID = nextPeriodID
}

// Clone returns a deep copy of the PeriodTracker suitable for use in a Buffer.
// The schedule and scheduleChanged signal are NOT cloned — they are Machine-level
// concerns that the Buffer never reads or writes.
func (pt *PeriodTracker) Clone() *PeriodTracker {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	clonedAll := make(map[uint64]*commonpb.Period, len(pt.allPeriods))
	for id, p := range pt.allPeriods {
		clonedAll[id] = proto.CloneOf(p)
	}

	var clonedOpen *commonpb.Period
	if pt.currentOpenPeriod != nil {
		clonedOpen = clonedAll[pt.currentOpenPeriod.GetId()]
	}

	var clonedClosing []*commonpb.Period
	for _, cp := range pt.closingPeriods {
		clonedClosing = append(clonedClosing, clonedAll[cp.GetId()])
	}

	return &PeriodTracker{
		allPeriods:        clonedAll,
		currentOpenPeriod: clonedOpen,
		closingPeriods:    clonedClosing,
		nextPeriodID:      pt.nextPeriodID,
	}
}
