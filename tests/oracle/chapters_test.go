package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

// chaptersOf builds a registry from statuses given in id order, starting at
// chapter 1.
func chaptersOf(t *testing.T, statuses ...ChapterStatus) Chapters {
	t.Helper()

	observed := map[uint64]ChapterStatus{}
	for i, s := range statuses {
		observed[uint64(i+1)] = s
	}

	c, err := ChaptersFrom(observed)
	require.NoError(t, err)

	return c
}

func stateOf(t *testing.T, statuses ...ChapterStatus) GlobalState {
	t.Helper()

	return NewGlobalState().WithChapters(chaptersOf(t, statuses...))
}

func TestChapters_NormalForm(t *testing.T) {
	t.Parallel()

	t.Run("no chapter until the first proposal bootstraps one", func(t *testing.T) {
		t.Parallel()

		c := NewChapters()
		require.Zero(t, c.OpenID())
		require.Zero(t, c.LastID())

		_, ok := c.StatusOf(1)
		require.False(t, ok)
	})

	t.Run("resolves every id in a mixed registry", func(t *testing.T) {
		t.Parallel()

		// Sealing is not ordered on the server (processSealChapter takes any
		// CLOSING chapter), so a sealed chapter above an unsealed one must be
		// representable.
		c := chaptersOf(t, ChapterArchived, ChapterArchived, ChapterArchiving, ChapterClosing, ChapterClosed, ChapterOpen)

		require.Equal(t, uint64(2), c.ArchivedThrough())
		require.True(t, c.Archiving())
		require.Equal(t, uint64(6), c.OpenID())
		require.Equal(t, uint64(6), c.LastID())

		for id, want := range map[uint64]ChapterStatus{
			1: ChapterArchived,
			2: ChapterArchived,
			3: ChapterArchiving,
			4: ChapterClosing,
			5: ChapterClosed,
			6: ChapterOpen,
		} {
			got, ok := c.StatusOf(id)
			require.True(t, ok, "chapter %d", id)
			require.Equal(t, want, got, "chapter %d", id)
		}

		_, ok := c.StatusOf(7)
		require.False(t, ok)
	})

	t.Run("rejects shapes the lifecycle cannot produce", func(t *testing.T) {
		t.Parallel()

		for name, observed := range map[string]map[uint64]ChapterStatus{
			"gap in the ids":              {1: ChapterArchived, 3: ChapterOpen},
			"archived above unarchived":   {1: ChapterClosed, 2: ChapterArchived, 3: ChapterOpen},
			"archiving off the successor": {1: ChapterClosed, 2: ChapterArchiving, 3: ChapterOpen},
			"two archiving":               {1: ChapterArchiving, 2: ChapterArchiving, 3: ChapterOpen},
			"open below the highest":      {1: ChapterOpen, 2: ChapterClosed},
		} {
			_, err := ChaptersFrom(observed)
			require.Error(t, err, name)
		}
	})
}

func TestChapters_Close(t *testing.T) {
	t.Parallel()

	t.Run("closes the open chapter and opens its successor", func(t *testing.T) {
		t.Parallel()

		res := stateOf(t, ChapterOpen).Apply(Bulk{Requests: []*servicepb.Request{actions.CloseChapterAction()}})
		require.True(t, res.OK)

		c := res.State.Chapters()
		require.Equal(t, uint64(2), c.OpenID())

		status, ok := c.StatusOf(1)
		require.True(t, ok)
		require.Equal(t, ChapterClosing, status)
	})

	t.Run("rejects with no chapter open", func(t *testing.T) {
		t.Parallel()

		res := NewGlobalState().Apply(Bulk{Requests: []*servicepb.Request{actions.CloseChapterAction()}})
		require.False(t, res.OK)
		require.Equal(t, domain.ErrReasonNoChapterOpen, res.Reason)
	})
}

func TestChapters_Archive(t *testing.T) {
	t.Parallel()

	archive := func(g GlobalState, id uint64) ApplyResult {
		return g.Apply(Bulk{Requests: []*servicepb.Request{actions.ArchiveChapterAction(id)}})
	}

	t.Run("accepts the prefix successor once it is sealed", func(t *testing.T) {
		t.Parallel()

		g := stateOf(t, ChapterClosed, ChapterClosed, ChapterOpen)
		res := archive(g, 1)
		require.True(t, res.OK)

		c := res.State.Chapters()
		require.True(t, c.Archiving())
		require.Zero(t, c.ArchivedThrough(), "the prefix extends on the confirm, not the request")

		status, ok := c.StatusOf(1)
		require.True(t, ok)
		require.Equal(t, ChapterArchiving, status)
	})

	t.Run("rejects an unsealed chapter", func(t *testing.T) {
		t.Parallel()

		res := archive(stateOf(t, ChapterClosing, ChapterOpen), 1)
		require.False(t, res.OK)
		require.Equal(t, domain.ErrReasonChapterNotClosed, res.Reason)
	})

	t.Run("rejects the open chapter", func(t *testing.T) {
		t.Parallel()

		res := archive(stateOf(t, ChapterOpen), 1)
		require.False(t, res.OK)
		require.Equal(t, domain.ErrReasonChapterNotClosed, res.Reason)
	})

	t.Run("rejects a chapter that never existed", func(t *testing.T) {
		t.Parallel()

		res := archive(stateOf(t, ChapterClosed, ChapterOpen), 9)
		require.False(t, res.OK)
		require.Equal(t, domain.ErrReasonChapterNotFound, res.Reason)
	})

	t.Run("rejects a jump over the successor", func(t *testing.T) {
		t.Parallel()

		res := archive(stateOf(t, ChapterClosed, ChapterClosed, ChapterOpen), 2)
		require.False(t, res.OK)
		require.Equal(t, domain.ErrReasonChapterArchiveOutOfOrder, res.Reason)
	})

	t.Run("rejects the successor while it is still archiving", func(t *testing.T) {
		t.Parallel()

		res := archive(stateOf(t, ChapterArchiving, ChapterClosed, ChapterOpen), 1)
		require.False(t, res.OK)
		require.Equal(t, domain.ErrReasonChapterNotClosed, res.Reason)
	})

	// The check order is observable, and it is what keeps a running node and a
	// restarted one auditing the same failure: an archived chapter is absent from
	// the running node's working set and present after recovery, so a
	// residency-first order would answer CHAPTER_NOT_FOUND on one and
	// CHAPTER_NOT_CLOSED on the other.
	t.Run("rejects a re-archive as already archived, not as missing", func(t *testing.T) {
		t.Parallel()

		res := archive(stateOf(t, ChapterArchived, ChapterClosed, ChapterOpen), 1)
		require.False(t, res.OK)
		require.Equal(t, domain.ErrReasonChapterAlreadyArchived, res.Reason)
	})

	// An unsealed chapter past the successor reports NOT_CLOSED while a sealed one
	// reports OUT_OF_ORDER, so every chapter's seal state is observable through the
	// reason — not only the frontier's.
	t.Run("distinguishes unsealed from sealed past the successor", func(t *testing.T) {
		t.Parallel()

		g := stateOf(t, ChapterClosed, ChapterClosing, ChapterClosed, ChapterOpen)

		unsealed := archive(g, 2)
		require.Equal(t, domain.ErrReasonChapterNotClosed, unsealed.Reason)

		sealed := archive(g, 3)
		require.Equal(t, domain.ErrReasonChapterArchiveOutOfOrder, sealed.Reason)
	})

	t.Run("a rejected order leaves the registry untouched", func(t *testing.T) {
		t.Parallel()

		g := stateOf(t, ChapterClosed, ChapterOpen)
		res := g.Apply(Bulk{Requests: []*servicepb.Request{
			actions.CloseChapterAction(),
			actions.ArchiveChapterAction(9),
		}})

		require.False(t, res.OK)
		require.Equal(t, domain.ErrReasonChapterNotFound, res.Reason)
		require.Equal(t, g.Chapters().Fingerprint(), res.State.Chapters().Fingerprint())
	})
}

func TestChapters_AutonomousTransitions(t *testing.T) {
	t.Parallel()

	t.Run("only CLOSING and ARCHIVING have an unrequested successor", func(t *testing.T) {
		t.Parallel()

		for status, want := range map[ChapterStatus]ChapterStatus{
			ChapterClosing:   ChapterClosed,
			ChapterArchiving: ChapterArchived,
		} {
			next, ok := AutonomousNext(status)
			require.True(t, ok, status)
			require.Equal(t, want, next)
		}

		for _, status := range []ChapterStatus{ChapterOpen, ChapterClosed, ChapterArchived} {
			_, ok := AutonomousNext(status)
			require.False(t, ok, status)
		}
	})

	t.Run("the seal makes an archive legal", func(t *testing.T) {
		t.Parallel()

		c := chaptersOf(t, ChapterClosing, ChapterOpen)

		sealed, ok := c.WithSealed(1)
		require.True(t, ok)

		status, found := sealed.StatusOf(1)
		require.True(t, found)
		require.Equal(t, ChapterClosed, status)

		_, notClosing := sealed.WithSealed(1)
		require.False(t, notClosing, "sealing an already-sealed chapter is not a step")
	})

	t.Run("the confirm extends the prefix", func(t *testing.T) {
		t.Parallel()

		c := chaptersOf(t, ChapterArchiving, ChapterClosed, ChapterOpen)

		confirmed, ok := c.WithConfirmed()
		require.True(t, ok)
		require.Equal(t, uint64(1), confirmed.ArchivedThrough())
		require.False(t, confirmed.Archiving())

		_, nothingArchiving := confirmed.WithConfirmed()
		require.False(t, nothingArchiving)
	})
}

// The registry's fingerprint must be a pure function of its contents: an archive
// pops the head of the sealed list and re-folds its index-keyed terms, so a
// registry reached by archiving has to fingerprint identically to the same
// registry observed directly. Otherwise candidateBases would treat one committed
// history as two distinct states.
func TestChapters_FingerprintIsContentAddressed(t *testing.T) {
	t.Parallel()

	g := stateOf(t, ChapterClosed, ChapterClosed, ChapterOpen)

	archiving := g.Apply(Bulk{Requests: []*servicepb.Request{actions.ArchiveChapterAction(1)}})
	require.True(t, archiving.OK)
	require.Equal(t,
		chaptersOf(t, ChapterArchiving, ChapterClosed, ChapterOpen).Fingerprint(),
		archiving.State.Chapters().Fingerprint())

	confirmed, ok := archiving.State.Chapters().WithConfirmed()
	require.True(t, ok)
	require.Equal(t,
		chaptersOf(t, ChapterArchived, ChapterClosed, ChapterOpen).Fingerprint(),
		confirmed.Fingerprint())
}

// Two registries that differ only in which chapter is sealed predict different
// reasons for the same archive request, so they must not collapse in the dedup.
func TestChapters_FingerprintSeparatesSealOrder(t *testing.T) {
	t.Parallel()

	require.NotEqual(t,
		chaptersOf(t, ChapterClosed, ChapterClosing, ChapterOpen).Fingerprint(),
		chaptersOf(t, ChapterClosing, ChapterClosed, ChapterOpen).Fingerprint())
}
