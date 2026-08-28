package main

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

func registry(t *testing.T, statuses ...oracle.ChapterStatus) oracle.Chapters {
	t.Helper()

	observed := map[uint64]oracle.ChapterStatus{}
	for i, s := range statuses {
		observed[uint64(i+1)] = s
	}

	c, err := oracle.ChaptersFrom(observed)
	require.NoError(t, err)

	return c
}

// The advance check is the lifecycle assertion: what separates the model from an
// observed registry must be the two transitions the server makes with no request
// behind it, plus whatever the driver's own outstanding orders asked for.
func TestChapterAdvanceExplained(t *testing.T) {
	t.Parallel()

	closed3 := func() oracle.Chapters {
		return registry(t, oracle.ChapterClosed, oracle.ChapterClosing, oracle.ChapterOpen)
	}

	for name, tc := range map[string]struct {
		pinned, observed oracle.Chapters
		closes           int
		archives         []uint64
		explained        bool
	}{
		"unchanged": {
			pinned: closed3(), observed: closed3(), explained: true,
		},
		"the Sealer sealed a closing chapter": {
			pinned:   closed3(),
			observed: registry(t, oracle.ChapterClosed, oracle.ChapterClosed, oracle.ChapterOpen),
			// No order seals a chapter, so this needs nothing outstanding.
			explained: true,
		},
		"the Archiver confirmed the archiving chapter": {
			pinned:    registry(t, oracle.ChapterArchiving, oracle.ChapterClosed, oracle.ChapterOpen),
			observed:  registry(t, oracle.ChapterArchived, oracle.ChapterClosed, oracle.ChapterOpen),
			explained: true,
		},
		"archiving started with the archive outstanding": {
			pinned:    closed3(),
			observed:  registry(t, oracle.ChapterArchiving, oracle.ChapterClosing, oracle.ChapterOpen),
			archives:  []uint64{1},
			explained: true,
		},
		"archived outright with the archive outstanding": {
			// The request moved it to ARCHIVING; the confirm that followed needed no
			// order of its own.
			pinned:    closed3(),
			observed:  registry(t, oracle.ChapterArchived, oracle.ChapterClosing, oracle.ChapterOpen),
			archives:  []uint64{1},
			explained: true,
		},
		"archiving started with nothing outstanding": {
			pinned:    closed3(),
			observed:  registry(t, oracle.ChapterArchiving, oracle.ChapterClosing, oracle.ChapterOpen),
			explained: false,
		},
		"archiving started for a chapter nobody asked about": {
			pinned:    registry(t, oracle.ChapterClosed, oracle.ChapterOpen),
			observed:  registry(t, oracle.ChapterArchiving, oracle.ChapterOpen),
			archives:  []uint64{2},
			explained: false,
		},
		"a chapter appeared with a close outstanding": {
			pinned:    registry(t, oracle.ChapterOpen),
			observed:  registry(t, oracle.ChapterClosing, oracle.ChapterOpen),
			closes:    1,
			explained: true,
		},
		"a chapter appeared with nothing outstanding": {
			pinned:    registry(t, oracle.ChapterOpen),
			observed:  registry(t, oracle.ChapterClosing, oracle.ChapterOpen),
			explained: false,
		},
		"two chapters appeared with one close outstanding": {
			pinned:    registry(t, oracle.ChapterOpen),
			observed:  registry(t, oracle.ChapterClosing, oracle.ChapterClosing, oracle.ChapterOpen),
			closes:    1,
			explained: false,
		},
		"a status moved backwards": {
			pinned:    registry(t, oracle.ChapterClosed, oracle.ChapterOpen),
			observed:  registry(t, oracle.ChapterClosing, oracle.ChapterOpen),
			explained: false,
		},
		"the archived prefix rewound": {
			pinned:    registry(t, oracle.ChapterArchived, oracle.ChapterClosed, oracle.ChapterOpen),
			observed:  registry(t, oracle.ChapterClosed, oracle.ChapterClosed, oracle.ChapterOpen),
			explained: false,
		},
		"a chapter disappeared": {
			pinned:    registry(t, oracle.ChapterClosed, oracle.ChapterOpen),
			observed:  registry(t, oracle.ChapterOpen),
			explained: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			violation, explained := chapterAdvanceExplained(tc.pinned, tc.observed, tc.closes, tc.archives)
			require.Equal(t, tc.explained, explained, "violation: %s", violation)
			if !explained {
				require.NotEmpty(t, violation, "a rejection must name its violation")
			}
		})
	}
}

// candidateBases branches over the unobserved seal of every chapter an order in
// play names, so the driver keeps that set small by refusing to grow it — the bound
// is arithmetic the emitter enforces, not a property of the pacing happening to
// stay ahead.
func TestTakeChapterOrder_WithholdsAtTheCeiling(t *testing.T) {
	t.Parallel()

	now := time.Unix(1, 0)

	c := NewChecker([]string{"L"}, nil)
	// Every draw off an hourly median outlasts the test; every draw off a
	// nanosecond one is already due.
	c.enableChapterOrders(time.Hour, time.Nanosecond)
	c.lastChapterClose = now

	require.Equal(t, chapterOrderArchive, c.takeChapterOrderLocked(now.Add(time.Second)),
		"nothing in play: the slot goes out")

	for i := range maxChaptersInPlay {
		c.inflight[uint64(100+i)] = bulkOf(actions.ArchiveChapterAction(uint64(i + 1)))
	}
	require.Equal(t, chapterOrderNone, c.takeChapterOrderLocked(now.Add(2*time.Second)),
		"at the ceiling: withheld")
	require.True(t, c.chapterSlotWithheld, "a withheld slot must be visible in the run")

	// Several orders naming one chapter are one branch, so they do not fill the set.
	c.inflight = map[uint64]oracle.Bulk{}
	for i := range maxChaptersInPlay + 2 {
		c.inflight[uint64(200+i)] = bulkOf(actions.ArchiveChapterAction(7))
	}
	require.Equal(t, chapterOrderArchive, c.takeChapterOrderLocked(now.Add(3*time.Second)))
}

// A prediction that turns on a pending unasked transition has to admit the
// registry that transition would have produced — otherwise a legitimately
// committed archive reads as a model divergence.
func TestApplyWithAutonomy(t *testing.T) {
	t.Parallel()

	archive := func(id uint64) oracle.Bulk {
		return oracle.Bulk{Requests: []*servicepb.Request{actions.ArchiveChapterAction(id)}}
	}

	t.Run("accepts an archive whose chapter the Sealer had already sealed", func(t *testing.T) {
		t.Parallel()

		base := oracle.NewGlobalState().WithChapters(registry(t, oracle.ChapterClosing, oracle.ChapterOpen))

		// The model has not seen the seal, so the plain prediction refuses.
		require.Equal(t, domain.ErrReasonChapterNotClosed, base.Apply(archive(1)).Reason)

		res, ok := applyWithAutonomy(base, archive(1), applyCommitted)
		require.True(t, ok)
		require.True(t, res.OK)

		// Adopting the result adopts the hypothesis: chapter 1 is now archiving.
		status, found := res.State.Chapters().StatusOf(1)
		require.True(t, found)
		require.Equal(t, oracle.ChapterArchiving, status)
	})

	t.Run("accepts the successor of a chapter the Archiver had already confirmed", func(t *testing.T) {
		t.Parallel()

		base := oracle.NewGlobalState().WithChapters(
			registry(t, oracle.ChapterArchiving, oracle.ChapterClosed, oracle.ChapterOpen))

		require.Equal(t, domain.ErrReasonChapterArchiveOutOfOrder, base.Apply(archive(2)).Reason)

		res, ok := applyWithAutonomy(base, archive(2), applyCommitted)
		require.True(t, ok)
		require.Equal(t, uint64(1), res.State.Chapters().ArchivedThrough())
	})

	t.Run("refuses an archive no pending transition could make legal", func(t *testing.T) {
		t.Parallel()

		base := oracle.NewGlobalState().WithChapters(registry(t, oracle.ChapterClosed, oracle.ChapterOpen))

		_, ok := applyWithAutonomy(base, archive(9), applyCommitted)
		require.False(t, ok, "chapter 9 never existed; no unasked transition invents it")
	})

	t.Run("a bulk with no chapter order costs one prediction", func(t *testing.T) {
		t.Parallel()

		pinned := registry(t, oracle.ChapterClosing, oracle.ChapterOpen)
		require.Len(t, chapterVariants(pinned, nil), 1)
	})
}

// The tail is the point: a fixed gap keeps the successor the only sealed chapter,
// so an archive request past it can never be answered out-of-order.
func TestSampleGap_MedianAndTail(t *testing.T) {
	t.Parallel()

	const (
		base    = 10 * time.Second
		samples = 20000
	)

	gaps := make([]time.Duration, 0, samples)
	for range samples {
		gaps = append(gaps, sampleGap(base))
	}
	slices.Sort(gaps)

	require.InEpsilon(t, float64(base), float64(gaps[samples/2]), 0.05,
		"the configured gap must stay the median, so the pacing knob keeps meaning what it says")

	var long, capped int
	for _, gap := range gaps {
		if gap > 4*base {
			long++
		}
		if gap >= chapterGapMaxMultiple*base {
			capped++
		}
	}

	require.Greater(t, long, samples/50, "the tail must reach four times the median often enough to stack two sealed chapters")
	require.Less(t, long, samples/5, "a tail that common would starve the accepted-archive path")
	require.Less(t, capped, samples/100, "the ceiling is a backstop against a wasted run, not the shape of the distribution")
}

func TestSampleGap_DisabledStaysDisabled(t *testing.T) {
	t.Parallel()

	require.Zero(t, sampleGap(0), "a run without cold storage emits no chapter orders at all")
}
