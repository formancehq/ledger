package backup

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func closeChapterLog(seq uint64, closed, opened *commonpb.Chapter) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CloseChapter{
				CloseChapter: &commonpb.ClosedChapterLog{
					ClosedChapter: closed,
					NewChapter:    opened,
				},
			},
		},
	}
}

func sealChapterLog(seq uint64, chapter *commonpb.Chapter) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_SealChapter{
				SealChapter: &commonpb.SealedChapterLog{Chapter: chapter},
			},
		},
	}
}

func archiveChapterLog(seq uint64, chapter *commonpb.Chapter) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_ArchiveChapter{
				ArchiveChapter: &commonpb.ArchivedChapterLog{Chapter: chapter},
			},
		},
	}
}

func confirmArchiveChapterLog(seq uint64, chapter *commonpb.Chapter) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_ConfirmArchiveChapter{
				ConfirmArchiveChapter: &commonpb.ConfirmedArchiveChapterLog{Chapter: chapter},
			},
		},
	}
}

func auditEntryWithHash(seq uint64, hash []byte) *auditpb.AuditEntry {
	return &auditpb.AuditEntry{
		Sequence: seq,
		Hash:     hash,
		Outcome: &auditpb.AuditEntry_Success{
			Success: &auditpb.AuditSuccess{},
		},
	}
}

func readChapterRows(t *testing.T, store *dal.Store) map[uint64]*commonpb.Chapter {
	t.Helper()

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	cursor, err := query.ReadChapters(context.Background(), handle)
	require.NoError(t, err)

	defer func() { _ = cursor.Close() }()

	rows := map[uint64]*commonpb.Chapter{}

	for {
		chapter, err := cursor.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		rows[chapter.GetId()] = chapter
	}

	return rows
}

func readNextChapterID(t *testing.T, store *dal.Store) uint64 {
	t.Helper()

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	next, err := query.ReadNextChapterID(handle)
	require.NoError(t, err)

	return next
}

// TestRebuildDelta_RebuildsChapterRegistry is the EN-1750 regression: chapter
// lifecycle logs in the delta must rebuild the registry rows and NextChapterID
// that FSM recovery reads at boot. Without them a restored node's registry is
// frozen at checkpoint time — the boot genesis path re-creates "chapter 1"
// over already-archived history and the next archival purges data that was
// never uploaded.
func TestRebuildDelta_RebuildsChapterRegistry(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	archived := &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      1,
		CloseSequence:      3,
		StartAuditSequence: 1,
		CloseAuditSequence: 2,
		SealingHash:        []byte("sealing-hash"),
		StateHash:          []byte("state-hash"),
		LastAuditHash:      []byte("hash-2"),
	}
	opened := &commonpb.Chapter{
		Id:                 2,
		Status:             commonpb.ChapterStatus_CHAPTER_OPEN,
		StartSequence:      4,
		StartAuditSequence: 3,
	}

	// The lifecycle as the live logs carry it: the close snapshot is CLOSING
	// with no hashes yet (LastAuditHash is stamped on the tracker's row after
	// the log snapshot is cloned), the later snapshots carry the accumulated
	// state.
	closing := archived.CloneVT()
	closing.Status = commonpb.ChapterStatus_CHAPTER_CLOSING
	closing.SealingHash = nil
	closing.StateHash = nil
	closing.LastAuditHash = nil

	closed := archived.CloneVT()
	closed.Status = commonpb.ChapterStatus_CHAPTER_CLOSED

	archiving := archived.CloneVT()
	archiving.Status = commonpb.ChapterStatus_CHAPTER_ARCHIVING

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldAuditKey(1), auditEntryWithHash(1, []byte("hash-1"))))
	require.NoError(t, batch.SetProto(coldAuditKey(2), auditEntryWithHash(2, []byte("hash-2"))))
	require.NoError(t, batch.SetProto(coldLogKey(3), closeChapterLog(3, closing, opened)))
	require.NoError(t, batch.SetProto(coldLogKey(4), sealChapterLog(4, closed)))
	require.NoError(t, batch.SetProto(coldLogKey(5), archiveChapterLog(5, archiving)))
	require.NoError(t, batch.SetProto(coldLogKey(6), confirmArchiveChapterLog(6, archived)))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), logging.Testing(), store, 0, 0))

	rows := readChapterRows(t, store)
	require.Len(t, rows, 2)

	got := rows[1]
	require.NotNil(t, got, "the archived chapter's registry row must be rebuilt")
	require.Equal(t, commonpb.ChapterStatus_CHAPTER_ARCHIVED, got.GetStatus())
	require.EqualValues(t, 1, got.GetStartSequence())
	require.EqualValues(t, 3, got.GetCloseSequence())
	require.EqualValues(t, 1, got.GetStartAuditSequence())
	require.EqualValues(t, 2, got.GetCloseAuditSequence())
	require.Equal(t, []byte("sealing-hash"), got.GetSealingHash())
	require.Equal(t, []byte("state-hash"), got.GetStateHash())
	require.Equal(t, []byte("hash-2"), got.GetLastAuditHash())

	successor := rows[2]
	require.NotNil(t, successor, "the chapter opened at close must be rebuilt")
	require.Equal(t, commonpb.ChapterStatus_CHAPTER_OPEN, successor.GetStatus())
	require.EqualValues(t, 4, successor.GetStartSequence())

	require.EqualValues(t, 3, readNextChapterID(t, store),
		"NextChapterID must advance past the opened chapter or the restored FSM re-allocates archived ids")
}

// TestRebuildDelta_MalformedChapterLog_FailsLoudly: a chapter lifecycle log
// with a missing snapshot is a corrupt log stream — silently skipping it would
// report a successful restore with an incomplete registry, the exact
// identity-collision seed the chapter replay exists to prevent.
func TestRebuildDelta_MalformedChapterLog_FailsLoudly(t *testing.T) {
	t.Parallel()

	for name, log := range map[string]*commonpb.Log{
		"close without snapshots": closeChapterLog(3, nil, nil),
		"seal without chapter":    sealChapterLog(3, nil),
		"archive without chapter": archiveChapterLog(3, nil),
		"confirm without chapter": confirmArchiveChapterLog(3, nil),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newRebuildTestStore(t)

			batch := store.OpenWriteSession()
			require.NoError(t, batch.SetProto(coldLogKey(3), log))
			require.NoError(t, batch.Commit())

			err := RebuildDelta(context.Background(), logging.Testing(), store, 0, 0)
			require.ErrorContains(t, err, "corrupt log stream")
		})
	}
}

// TestRebuildDelta_CloseOnlyDelta_RecoversLastAuditHash: a chapter closed but
// not yet sealed at backup time carries no LastAuditHash in its close log (the
// live apply stamps it on the tracker's row after the snapshot is cloned). The
// rebuild must recover it from the stored audit entry at CloseAuditSequence —
// it is the chain seed the checker uses across the chapter's eventual purge,
// and no later lifecycle log exists in the delta to carry it.
func TestRebuildDelta_CloseOnlyDelta_RecoversLastAuditHash(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	closing := &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_CLOSING,
		StartSequence:      1,
		CloseSequence:      3,
		StartAuditSequence: 1,
		CloseAuditSequence: 2,
	}
	opened := &commonpb.Chapter{
		Id:                 2,
		Status:             commonpb.ChapterStatus_CHAPTER_OPEN,
		StartSequence:      4,
		StartAuditSequence: 3,
	}

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldAuditKey(1), auditEntryWithHash(1, []byte("hash-1"))))
	require.NoError(t, batch.SetProto(coldAuditKey(2), auditEntryWithHash(2, []byte("boundary-hash"))))
	require.NoError(t, batch.SetProto(coldLogKey(3), closeChapterLog(3, closing, opened)))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), logging.Testing(), store, 0, 0))

	rows := readChapterRows(t, store)

	got := rows[1]
	require.NotNil(t, got)
	require.Equal(t, commonpb.ChapterStatus_CHAPTER_CLOSING, got.GetStatus())
	require.Equal(t, []byte("boundary-hash"), got.GetLastAuditHash(),
		"the close-boundary audit entry's hash must be recovered onto the rebuilt row")

	require.EqualValues(t, 3, readNextChapterID(t, store))
}
