package backup

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func writeLogEntry(t *testing.T, store *dal.Store, seq uint64) {
	t.Helper()

	batch := store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(seq).Build()
	require.NoError(t, batch.SetProto(key, &commonpb.Log{Sequence: seq}))
	require.NoError(t, batch.Commit())
}

func writeAppliedProposal(t *testing.T, store *dal.Store, seq uint64) {
	t.Helper()

	batch := store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAppliedProposal).PutUint64(seq).Build()
	require.NoError(t, batch.SetBytes(key, []byte("proposal")))
	require.NoError(t, batch.Commit())
}

func writeChapterRow(t *testing.T, store *dal.Store, chapter *commonpb.Chapter) {
	t.Helper()

	batch := store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneGlobal, dal.SubGlobChapters).PutUint64(chapter.GetId()).Build()
	require.NoError(t, batch.SetProto(key, chapter))
	require.NoError(t, batch.Commit())
}

// purgeChapterRanges range-deletes a chapter's log and audit entries from the
// hot store, mirroring what the FSM's ConfirmArchiveChapter purge executes.
func purgeChapterRanges(t *testing.T, store *dal.Store, chapter *commonpb.Chapter) {
	t.Helper()

	batch := store.OpenWriteSession()

	logStart := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(chapter.GetStartSequence()).Build()
	logEnd := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(chapter.GetCloseSequence() + 1).Build()
	require.NoError(t, batch.DeleteRange(logStart, logEnd, nil))

	for _, sub := range []byte{dal.SubColdAudit, dal.SubColdAuditItem, dal.SubColdAppliedProposal} {
		start := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, sub).PutUint64(chapter.GetStartAuditSequence()).Build()
		end := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, sub).PutUint64(chapter.GetCloseAuditSequence() + 1).Build()
		require.NoError(t, batch.DeleteRange(start, end, nil))
	}

	require.NoError(t, batch.Commit())
}

// coldReaderFor returns a mock ColdChapterReader serving the given store's
// content for exactly one chapter ID.
func coldReaderFor(t *testing.T, ctrl *gomock.Controller, chapterID uint64, coldStore *dal.Store) *MockColdChapterReader {
	t.Helper()

	handle, err := coldStore.NewDirectReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })

	reader := NewMockColdChapterReader(ctrl)
	reader.EXPECT().
		GetReader(gomock.Any(), chapterID).
		Return(handle, nil).
		AnyTimes()

	return reader
}

func readLogSequences(t *testing.T, store *dal.Store, afterSeq uint64) []uint64 {
	t.Helper()

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	cursor, err := query.ReadLogsSince(context.Background(), handle, afterSeq)
	require.NoError(t, err)

	defer func() { _ = cursor.Close() }()

	var seqs []uint64

	for {
		log, err := cursor.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		seqs = append(seqs, log.GetSequence())
	}

	return seqs
}

func countKeysInSub(t *testing.T, store *dal.Store, sub byte) int {
	t.Helper()

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	iter, err := dal.NewBoundedIter(handle,
		[]byte{dal.ZoneCold, sub},
		[]byte{dal.ZoneCold, sub + 1},
	)
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	require.NoError(t, iter.Error())

	return count
}

// archivedChapterFixture builds a hot store where chapter 1 (logs [1,7],
// audits [1,4]) is ARCHIVED and purged, hot activity continues to log 12 /
// audit 8, and a cold store holds the chapter's purged entries. Audit seqs 2
// and 4 are successes carrying an item and an applied proposal; 1 and 3 are
// failures, which also carry one item each (LogSequence = 0) but no proposal.
func archivedChapterFixture(t *testing.T) (hot, cold *dal.Store, chapter *commonpb.Chapter) {
	t.Helper()

	chapter = &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      1,
		CloseSequence:      7,
		StartAuditSequence: 1,
		CloseAuditSequence: 4,
	}

	hot = newBackupTestStore(t)
	for seq := uint64(1); seq <= 12; seq++ {
		writeLogEntry(t, hot, seq)
	}

	writeFailureAuditEntry(t, hot, 1)
	writeSuccessAuditEntryWithItem(t, hot, 2)
	writeAppliedProposal(t, hot, 2)
	writeFailureAuditEntry(t, hot, 3)
	writeSuccessAuditEntryWithItem(t, hot, 4)
	writeAppliedProposal(t, hot, 4)

	for seq := uint64(5); seq <= 8; seq++ {
		writeSuccessAuditEntryWithItem(t, hot, seq)
	}

	writeChapterRow(t, hot, chapter)
	purgeChapterRanges(t, hot, chapter)

	cold = newBackupTestStore(t)
	for seq := uint64(1); seq <= 7; seq++ {
		writeLogEntry(t, cold, seq)
	}

	writeFailureAuditEntry(t, cold, 1)
	writeSuccessAuditEntryWithItem(t, cold, 2)
	writeAppliedProposal(t, cold, 2)
	writeFailureAuditEntry(t, cold, 3)
	writeSuccessAuditEntryWithItem(t, cold, 4)
	writeAppliedProposal(t, cold, 4)

	return hot, cold, chapter
}

// TestRunIncrementalBackup_BackfillsArchivedRangesFromCold is the EN-1598
// regression: an incremental window spanning a chapter archival must backfill
// the purged range from the chapter's cold SST instead of silently skipping
// it and advancing the manifest floor past it. The restored store must hold
// every entry of the window.
func TestRunIncrementalBackup_BackfillsArchivedRangesFromCold(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	hot, cold, chapter := archivedChapterFixture(t)

	storage := newInMemoryBackupStorage()
	require.NoError(t, WriteManifest(context.Background(), storage, ManifestKey(bucketID), &Manifest{
		Checkpoint: &CheckpointManifest{LastLogSequence: 2, LastAuditSequence: 1},
	}))

	coldReader := coldReaderFor(t, gomock.NewController(t), chapter.GetId(), cold)

	result, err := RunIncrementalBackup(context.Background(), logging.Testing(), hot, coldReader, storage, bucketID, 0)
	require.NoError(t, err)
	require.EqualValues(t, 10, result.LogEntriesExported, "logs 3..12: cold [3,7] + hot [8,12]")
	require.EqualValues(t, 7, result.AuditEntriesExported, "audits 2..8: cold [2,4] + hot [5,8]")
	require.EqualValues(t, 12, result.LastLogSequence)
	require.EqualValues(t, 8, result.LastAuditSequence)

	manifest, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)

	// Cold segments must precede hot ones: LastExport*Sequence takes the last
	// segment of each type as the next run's floor.
	var logSegs []ExportSegment

	for _, seg := range manifest.Exports {
		if seg.Type == "log" {
			logSegs = append(logSegs, seg)
		}
	}

	require.Len(t, logSegs, 2)
	require.EqualValues(t, 3, logSegs[0].StartSeq)
	require.EqualValues(t, 7, logSegs[0].EndSeq)
	require.EqualValues(t, 8, logSegs[1].StartSeq)
	require.EqualValues(t, 12, logSegs[1].EndSeq)
	require.EqualValues(t, 12, manifest.LastExportLogSequence())
	require.EqualValues(t, 8, manifest.LastExportAuditSequence())

	restored := newBackupTestStore(t)
	require.NoError(t, ApplyExports(context.Background(), logging.Testing(), storage, restored, manifest.Exports))

	require.Equal(t, []uint64{3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, readLogSequences(t, restored, 2),
		"restored store must hold the archived range — pre-fix it silently lost logs 3..7")
	require.Equal(t, 7, countKeysInSub(t, restored, dal.SubColdAudit), "audits 2..8")
	require.Equal(t, 7, countKeysInSub(t, restored, dal.SubColdAuditItem), "items at 2, 3, 4, 5..8 (every audit entry writes one, including the failure at 3)")
	require.Equal(t, 2, countKeysInSub(t, restored, dal.SubColdAppliedProposal), "proposals at 2, 4")
}

// TestRunIncrementalBackup_ArchivedOverlapWithoutColdReader_FailsLoudly locks
// the other half of EN-1598: when the purged range cannot be backfilled
// (no cold storage configured), the backup must fail instead of publishing a
// manifest whose floor has advanced past entries it does not contain.
func TestRunIncrementalBackup_ArchivedOverlapWithoutColdReader_FailsLoudly(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	hot, _, _ := archivedChapterFixture(t)

	storage := newInMemoryBackupStorage()
	require.NoError(t, WriteManifest(context.Background(), storage, ManifestKey(bucketID), &Manifest{
		Checkpoint: &CheckpointManifest{LastLogSequence: 2, LastAuditSequence: 1},
	}))

	_, err := RunIncrementalBackup(context.Background(), logging.Testing(), hot, nil, storage, bucketID, 0)
	require.ErrorContains(t, err, "archived chapter 1")

	manifest, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	require.Empty(t, manifest.Exports, "a failed run must not publish export segments")
}

// TestRunIncrementalBackup_ArchivedChapterBelowFloor_NoColdRead: a chapter
// archived entirely below the export floor is already in the backup chain —
// the backfill must not touch cold storage for it (the strict mock fails on
// any GetReader call), so archived history is never re-exported run after run.
func TestRunIncrementalBackup_ArchivedChapterBelowFloor_NoColdRead(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	hot, _, chapter := archivedChapterFixture(t)

	storage := newInMemoryBackupStorage()
	require.NoError(t, WriteManifest(context.Background(), storage, ManifestKey(bucketID), &Manifest{
		Checkpoint: &CheckpointManifest{
			LastLogSequence:   chapter.GetCloseSequence(),
			LastAuditSequence: chapter.GetCloseAuditSequence(),
		},
	}))

	coldReader := NewMockColdChapterReader(gomock.NewController(t))

	result, err := RunIncrementalBackup(context.Background(), logging.Testing(), hot, coldReader, storage, bucketID, 0)
	require.NoError(t, err)
	require.EqualValues(t, 5, result.LogEntriesExported, "hot logs 8..12 only")

	manifest, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)

	for _, seg := range manifest.Exports {
		floor := chapter.GetCloseAuditSequence()
		if seg.Type == "log" {
			floor = chapter.GetCloseSequence()
		}

		require.Greater(t, seg.StartSeq, floor,
			"no segment may re-export the already-backed-up archived range")
	}
}

// TestRunIncrementalBackup_TruncatedColdArchive_FailsLoudly: the log stream is
// gap-free, so a cold archive holding fewer entries than the chapter registry
// claims is truncated or mismatched — exporting it would publish a backup
// missing entries the purge already deleted from hot. The run must fail before
// writing a manifest.
func TestRunIncrementalBackup_TruncatedColdArchive_FailsLoudly(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	hot, cold, chapter := archivedChapterFixture(t)

	// Punch a hole in the cold archive: log 5 disappears.
	batch := cold.OpenWriteSession()
	holeStart := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(5).Build()
	holeEnd := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(6).Build()
	require.NoError(t, batch.DeleteRange(holeStart, holeEnd, nil))
	require.NoError(t, batch.Commit())

	storage := newInMemoryBackupStorage()
	require.NoError(t, WriteManifest(context.Background(), storage, ManifestKey(bucketID), &Manifest{
		Checkpoint: &CheckpointManifest{LastLogSequence: 2, LastAuditSequence: 1},
	}))

	coldReader := coldReaderFor(t, gomock.NewController(t), chapter.GetId(), cold)

	_, err := RunIncrementalBackup(context.Background(), logging.Testing(), hot, coldReader, storage, bucketID, 0)
	require.ErrorContains(t, err, "invariant: cold archive for chapter 1 holds 4 log entries in [3, 7], expected 5")

	manifest, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	require.Empty(t, manifest.Exports, "a failed run must not publish export segments")
}
