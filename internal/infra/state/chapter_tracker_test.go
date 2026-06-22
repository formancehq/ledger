package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func newTestChapterTracker() *ChapterTracker {
	return NewChapterTracker(
		make(map[uint64]*commonpb.Chapter),
		nil, nil, 1, "",
	)
}

func TestChapterTrackerNewAndDefaults(t *testing.T) {
	t.Parallel()

	pt := newTestChapterTracker()

	require.Empty(t, pt.AllChapters())
	require.Nil(t, pt.CurrentOpenChapter())
	require.Empty(t, pt.ClosingChapters())
	require.Equal(t, uint64(1), pt.NextChapterID())
	require.Empty(t, pt.Schedule())
	require.NotNil(t, pt.ScheduleChanged())
}

func TestChapterTrackerNewWithState(t *testing.T) {
	t.Parallel()

	open := &commonpb.Chapter{Id: 1, Status: commonpb.ChapterStatus_CHAPTER_OPEN}
	closing := &commonpb.Chapter{Id: 2, Status: commonpb.ChapterStatus_CHAPTER_CLOSING}
	all := map[uint64]*commonpb.Chapter{1: open, 2: closing}

	pt := NewChapterTracker(all, open, []*commonpb.Chapter{closing}, 3, "0 * * * *")

	require.Len(t, pt.AllChapters(), 2)
	require.Equal(t, open, pt.CurrentOpenChapter())
	require.Len(t, pt.ClosingChapters(), 1)
	require.Equal(t, closing, pt.ClosingChapters()[0])
	require.Equal(t, uint64(3), pt.NextChapterID())
	require.Equal(t, "0 * * * *", pt.Schedule())
}

func TestChapterTrackerPutAndGet(t *testing.T) {
	t.Parallel()

	pt := newTestChapterTracker()

	_, ok := pt.GetChapterByID(10)
	require.False(t, ok)

	p := &commonpb.Chapter{Id: 10, Status: commonpb.ChapterStatus_CHAPTER_CLOSED}
	pt.PutChapter(p)

	got, ok := pt.GetChapterByID(10)
	require.True(t, ok)
	require.Equal(t, p, got)
	require.Len(t, pt.AllChapters(), 1)
}

func TestChapterTrackerDeleteChapter(t *testing.T) {
	t.Parallel()

	pt := newTestChapterTracker()
	pt.PutChapter(&commonpb.Chapter{Id: 5})
	pt.PutChapter(&commonpb.Chapter{Id: 6})
	require.Len(t, pt.AllChapters(), 2)

	pt.DeleteChapter(5)

	_, ok := pt.GetChapterByID(5)
	require.False(t, ok)
	require.Len(t, pt.AllChapters(), 1)
}

func TestChapterTrackerSetCurrentOpenChapter(t *testing.T) {
	t.Parallel()

	pt := newTestChapterTracker()
	require.Nil(t, pt.CurrentOpenChapter())

	p := &commonpb.Chapter{Id: 1, Status: commonpb.ChapterStatus_CHAPTER_OPEN}
	pt.SetCurrentOpenChapter(p)

	require.Equal(t, p, pt.CurrentOpenChapter())
	// SetCurrentOpenChapter also adds to allChapters
	got, ok := pt.GetChapterByID(1)
	require.True(t, ok)
	require.Equal(t, p, got)

	// Setting nil clears the open chapter but does not add to allChapters
	pt.SetCurrentOpenChapter(nil)
	require.Nil(t, pt.CurrentOpenChapter())
	// chapter 1 should still be in allChapters
	_, ok = pt.GetChapterByID(1)
	require.True(t, ok)
}

func TestChapterTrackerClosingChaptersLifecycle(t *testing.T) {
	t.Parallel()

	pt := newTestChapterTracker()
	require.Empty(t, pt.ClosingChapters())

	p1 := &commonpb.Chapter{Id: 3, Status: commonpb.ChapterStatus_CHAPTER_CLOSING}
	pt.AddClosingChapter(p1)
	require.Len(t, pt.ClosingChapters(), 1)

	cp, ok := pt.ClosingChapterByID(3)
	require.True(t, ok)
	require.Equal(t, p1, cp)

	// LatestClosingChapter returns the last added
	require.Equal(t, p1, pt.LatestClosingChapter())

	// Add a second
	p2 := &commonpb.Chapter{Id: 4, Status: commonpb.ChapterStatus_CHAPTER_CLOSING}
	pt.AddClosingChapter(p2)
	require.Len(t, pt.ClosingChapters(), 2)
	require.Equal(t, p2, pt.LatestClosingChapter())

	// Remove first by ID
	pt.RemoveClosingChapter(3)
	require.Len(t, pt.ClosingChapters(), 1)
	_, ok = pt.ClosingChapterByID(3)
	require.False(t, ok)

	// Remove second
	pt.RemoveClosingChapter(4)
	require.Empty(t, pt.ClosingChapters())
	require.Nil(t, pt.LatestClosingChapter())
}

func TestChapterTrackerRemoveClosingChapterNonExistent(t *testing.T) {
	t.Parallel()

	pt := newTestChapterTracker()
	p := &commonpb.Chapter{Id: 1, Status: commonpb.ChapterStatus_CHAPTER_CLOSING}
	pt.AddClosingChapter(p)

	// Removing a non-existent ID is a no-op
	pt.RemoveClosingChapter(999)
	require.Len(t, pt.ClosingChapters(), 1)
}

func TestChapterTrackerClosingChapterByIDNotFound(t *testing.T) {
	t.Parallel()

	pt := newTestChapterTracker()
	pt.AddClosingChapter(&commonpb.Chapter{Id: 5})

	_, ok := pt.ClosingChapterByID(99)
	require.False(t, ok)
}

func TestChapterTrackerSetClosingChaptersBulk(t *testing.T) {
	t.Parallel()

	pt := newTestChapterTracker()
	pt.AddClosingChapter(&commonpb.Chapter{Id: 1})

	// Bulk replace
	newChapters := []*commonpb.Chapter{
		{Id: 10, Status: commonpb.ChapterStatus_CHAPTER_CLOSING},
		{Id: 11, Status: commonpb.ChapterStatus_CHAPTER_CLOSING},
	}
	pt.SetClosingChapters(newChapters)

	require.Len(t, pt.ClosingChapters(), 2)
	_, ok := pt.ClosingChapterByID(1)
	require.False(t, ok)
	_, ok = pt.ClosingChapterByID(10)
	require.True(t, ok)
	_, ok = pt.ClosingChapterByID(11)
	require.True(t, ok)
}

func TestChapterTrackerCloneMultipleClosingChapters(t *testing.T) {
	t.Parallel()

	p1 := &commonpb.Chapter{Id: 1, Status: commonpb.ChapterStatus_CHAPTER_CLOSING, CloseSequence: 10}
	p2 := &commonpb.Chapter{Id: 2, Status: commonpb.ChapterStatus_CHAPTER_CLOSING, CloseSequence: 20}
	all := map[uint64]*commonpb.Chapter{1: p1, 2: p2}

	pt := NewChapterTracker(all, nil, []*commonpb.Chapter{p1, p2}, 5, "")
	clone := pt.Clone()

	require.Len(t, clone.ClosingChapters(), 2)
	require.Equal(t, uint64(1), clone.ClosingChapters()[0].GetId())
	require.Equal(t, uint64(2), clone.ClosingChapters()[1].GetId())

	// Mutating clone doesn't affect original
	clone.ClosingChapters()[0].CloseSequence = 999
	require.Equal(t, uint64(10), pt.ClosingChapters()[0].GetCloseSequence())

	clone.RemoveClosingChapter(1)
	require.Len(t, clone.ClosingChapters(), 1)
	require.Len(t, pt.ClosingChapters(), 2)
}

func TestChapterTrackerResetMultipleClosingChapters(t *testing.T) {
	t.Parallel()

	pt := newTestChapterTracker()

	c1 := &commonpb.Chapter{Id: 10, Status: commonpb.ChapterStatus_CHAPTER_CLOSING}
	c2 := &commonpb.Chapter{Id: 11, Status: commonpb.ChapterStatus_CHAPTER_CLOSING}
	newAll := map[uint64]*commonpb.Chapter{10: c1, 11: c2}

	pt.Reset(newAll, nil, []*commonpb.Chapter{c1, c2}, 20)

	require.Len(t, pt.ClosingChapters(), 2)
	require.Equal(t, c2, pt.LatestClosingChapter())
}

func TestChapterTrackerNextChapterID(t *testing.T) {
	t.Parallel()

	pt := newTestChapterTracker()
	require.Equal(t, uint64(1), pt.NextChapterID())

	pt.SetNextChapterID(42)
	require.Equal(t, uint64(42), pt.NextChapterID())
}

func TestChapterTrackerSchedule(t *testing.T) {
	t.Parallel()

	pt := newTestChapterTracker()
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

func TestChapterTrackerReset(t *testing.T) {
	t.Parallel()

	pt := newTestChapterTracker()
	pt.PutChapter(&commonpb.Chapter{Id: 1})
	pt.SetSchedule("@daily")
	require.Equal(t, "@daily", pt.Schedule())

	newOpen := &commonpb.Chapter{Id: 10, Status: commonpb.ChapterStatus_CHAPTER_OPEN}
	newClosing := &commonpb.Chapter{Id: 11, Status: commonpb.ChapterStatus_CHAPTER_CLOSING}
	newAll := map[uint64]*commonpb.Chapter{10: newOpen, 11: newClosing}

	pt.Reset(newAll, newOpen, []*commonpb.Chapter{newClosing}, 12)

	require.Len(t, pt.AllChapters(), 2)
	require.Equal(t, newOpen, pt.CurrentOpenChapter())
	require.Len(t, pt.ClosingChapters(), 1)
	require.Equal(t, newClosing, pt.ClosingChapters()[0])
	require.Equal(t, uint64(12), pt.NextChapterID())
	// Schedule is preserved across Reset (Machine-level concern)
	require.Equal(t, "@daily", pt.Schedule())
	// Old chapter is gone
	_, ok := pt.GetChapterByID(1)
	require.False(t, ok)
}

func TestChapterTrackerClone(t *testing.T) {
	t.Parallel()

	open := &commonpb.Chapter{Id: 1, Status: commonpb.ChapterStatus_CHAPTER_OPEN, StartSequence: 10}
	closing := &commonpb.Chapter{Id: 2, Status: commonpb.ChapterStatus_CHAPTER_CLOSING, CloseSequence: 20}
	closed := &commonpb.Chapter{Id: 3, Status: commonpb.ChapterStatus_CHAPTER_CLOSED}
	all := map[uint64]*commonpb.Chapter{1: open, 2: closing, 3: closed}

	pt := NewChapterTracker(all, open, []*commonpb.Chapter{closing}, 4, "*/10 * * * *")
	clone := pt.Clone()

	// Clone has same data
	require.Len(t, clone.AllChapters(), 3)
	require.Equal(t, uint64(4), clone.NextChapterID())
	require.NotNil(t, clone.CurrentOpenChapter())
	require.Equal(t, uint64(1), clone.CurrentOpenChapter().GetId())
	require.Len(t, clone.ClosingChapters(), 1)
	require.Equal(t, uint64(2), clone.ClosingChapters()[0].GetId())

	// Clone is a deep copy — mutating clone doesn't affect original
	clone.CurrentOpenChapter().StartSequence = 999
	require.Equal(t, uint64(10), pt.CurrentOpenChapter().GetStartSequence())

	clone.PutChapter(&commonpb.Chapter{Id: 100})
	require.Len(t, clone.AllChapters(), 4)
	require.Len(t, pt.AllChapters(), 3)

	clone.SetNextChapterID(99)
	require.Equal(t, uint64(4), pt.NextChapterID())

	// Schedule is NOT cloned
	require.Empty(t, clone.Schedule())
}

func TestChapterTrackerCloneNilChapters(t *testing.T) {
	t.Parallel()

	pt := newTestChapterTracker()
	pt.PutChapter(&commonpb.Chapter{Id: 5, Status: commonpb.ChapterStatus_CHAPTER_CLOSED})

	clone := pt.Clone()

	require.Nil(t, clone.CurrentOpenChapter())
	require.Empty(t, clone.ClosingChapters())
	require.Len(t, clone.AllChapters(), 1)
}
