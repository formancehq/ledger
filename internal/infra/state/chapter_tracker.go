package state

import (
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// ChapterTracker encapsulates all chapter-related state: the map of all
// non-purged chapters, convenience pointers to the open/closing chapters,
// the auto-incrementing chapter ID, and the cron schedule.
type ChapterTracker struct {
	mu                 sync.RWMutex
	allChapters        map[uint64]*commonpb.Chapter
	currentOpenChapter *commonpb.Chapter
	closingChapters    []*commonpb.Chapter
	nextChapterID      uint64

	// schedule is the cron expression for automatic chapter rotation (empty = disabled).
	schedule string
	// scheduleChanged fires when the schedule is updated via Raft.
	scheduleChanged signal.Signal
}

// NewChapterTracker creates a ChapterTracker from persisted state loaded at startup.
func NewChapterTracker(
	allChapters map[uint64]*commonpb.Chapter,
	currentOpenChapter *commonpb.Chapter,
	closingChapters []*commonpb.Chapter,
	nextChapterID uint64,
	schedule string,
) *ChapterTracker {
	if allChapters == nil {
		allChapters = map[uint64]*commonpb.Chapter{}
	}

	return &ChapterTracker{
		allChapters:        allChapters,
		currentOpenChapter: currentOpenChapter,
		closingChapters:    closingChapters,
		nextChapterID:      nextChapterID,
		schedule:           schedule,
		scheduleChanged:    signal.New(),
	}
}

// AllChapters returns a slice of all non-purged chapters.
func (pt *ChapterTracker) AllChapters() []*commonpb.Chapter {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	chapters := make([]*commonpb.Chapter, 0, len(pt.allChapters))
	for _, p := range pt.allChapters {
		chapters = append(chapters, p)
	}

	return chapters
}

// CurrentOpenChapter returns the chapter currently in OPEN state, or nil.
func (pt *ChapterTracker) CurrentOpenChapter() *commonpb.Chapter {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return pt.currentOpenChapter
}

// SetCurrentOpenChapter sets the current open chapter.
func (pt *ChapterTracker) SetCurrentOpenChapter(p *commonpb.Chapter) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.currentOpenChapter = p
	if p != nil {
		pt.allChapters[p.GetId()] = p
	}
}

// ClosingChapters returns all chapters currently in CLOSING state.
func (pt *ChapterTracker) ClosingChapters() []*commonpb.Chapter {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return pt.closingChapters
}

// ClosingChapterByID returns the closing chapter with the given ID, if any.
func (pt *ChapterTracker) ClosingChapterByID(id uint64) (*commonpb.Chapter, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	for _, p := range pt.closingChapters {
		if p.GetId() == id {
			return p, true
		}
	}

	return nil, false
}

// LatestClosingChapter returns the most recently added closing chapter, or nil.
func (pt *ChapterTracker) LatestClosingChapter() *commonpb.Chapter {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	if len(pt.closingChapters) == 0 {
		return nil
	}

	return pt.closingChapters[len(pt.closingChapters)-1]
}

// AddClosingChapter appends a chapter to the closing chapters list.
func (pt *ChapterTracker) AddClosingChapter(p *commonpb.Chapter) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.closingChapters = append(pt.closingChapters, p)
}

// RemoveClosingChapter removes the closing chapter with the given ID.
func (pt *ChapterTracker) RemoveClosingChapter(id uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	for i, p := range pt.closingChapters {
		if p.GetId() == id {
			pt.closingChapters = append(pt.closingChapters[:i], pt.closingChapters[i+1:]...)

			return
		}
	}
}

// SetClosingChapters replaces the entire closing chapters slice (used during Commit).
func (pt *ChapterTracker) SetClosingChapters(chapters []*commonpb.Chapter) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.closingChapters = chapters
}

// GetChapterByID looks up a chapter by ID.
func (pt *ChapterTracker) GetChapterByID(id uint64) (*commonpb.Chapter, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	p, ok := pt.allChapters[id]

	return p, ok
}

// PutChapter adds or updates a chapter in the map.
func (pt *ChapterTracker) PutChapter(p *commonpb.Chapter) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.allChapters[p.GetId()] = p
}

// DeleteChapter removes a chapter from the map.
func (pt *ChapterTracker) DeleteChapter(id uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	delete(pt.allChapters, id)
}

// NextChapterID returns the next chapter ID to assign.
func (pt *ChapterTracker) NextChapterID() uint64 {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return pt.nextChapterID
}

// SetNextChapterID sets the next chapter ID.
func (pt *ChapterTracker) SetNextChapterID(id uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.nextChapterID = id
}

// Schedule returns the cron expression for automatic chapter rotation.
func (pt *ChapterTracker) Schedule() string {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return pt.schedule
}

// SetSchedule updates the schedule and fires the changed signal.
func (pt *ChapterTracker) SetSchedule(s string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.schedule = s
	pt.scheduleChanged.Notify()
}

// ScheduleChanged returns the Signal that fires when the schedule is updated.
func (pt *ChapterTracker) ScheduleChanged() signal.Signal {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return pt.scheduleChanged
}

// Reset rebuilds the tracker from a snapshot's chapter data.
func (pt *ChapterTracker) Reset(
	allChapters map[uint64]*commonpb.Chapter,
	currentOpenChapter *commonpb.Chapter,
	closingChapters []*commonpb.Chapter,
	nextChapterID uint64,
) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.allChapters = allChapters
	pt.currentOpenChapter = currentOpenChapter
	pt.closingChapters = closingChapters
	pt.nextChapterID = nextChapterID
}

// Clone returns a deep copy of the ChapterTracker suitable for use in a Buffer.
// The schedule and scheduleChanged signal are NOT cloned — they are Machine-level
// concerns that the Buffer never reads or writes.
func (pt *ChapterTracker) Clone() *ChapterTracker {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	clonedAll := make(map[uint64]*commonpb.Chapter, len(pt.allChapters))
	for id, p := range pt.allChapters {
		clonedAll[id] = proto.CloneOf(p)
	}

	var clonedOpen *commonpb.Chapter
	if pt.currentOpenChapter != nil {
		clonedOpen = clonedAll[pt.currentOpenChapter.GetId()]
	}

	var clonedClosing []*commonpb.Chapter
	for _, cp := range pt.closingChapters {
		clonedClosing = append(clonedClosing, clonedAll[cp.GetId()])
	}

	return &ChapterTracker{
		allChapters:        clonedAll,
		currentOpenChapter: clonedOpen,
		closingChapters:    clonedClosing,
		nextChapterID:      pt.nextChapterID,
	}
}
