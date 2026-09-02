package backup

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// writeColdEntry puts one byte-valued row under the given cold sub-prefix, so a
// test can observe presence per range without building real payloads.
func writeColdEntry(t *testing.T, store *dal.Store, sub byte, seq uint64) {
	t.Helper()

	batch := store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, sub).PutUint64(seq).Build()
	require.NoError(t, batch.SetBytes(key, []byte("x")))
	require.NoError(t, batch.Commit())
}

// A restore re-ingests the archived ranges its export carried, because the
// rebuild needs them. Past the rebuild the source held them in cold storage
// alone, and so must the restored store: reads try hot first, the disk otherwise
// keeps a second copy, and the exclusion pass finds purge receipts its replay
// never derives.
func TestPurgeArchivedRanges_RemovesTheReIngestedRange(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)
	ctx := context.Background()

	writeChapterRow(t, store, &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      1,
		CloseSequence:      100,
		StartAuditSequence: 1,
		CloseAuditSequence: 40,
	})

	// Inside the archived range, as the export re-ingested them.
	writeColdEntry(t, store, dal.SubColdLog, 50)
	writeColdEntry(t, store, dal.SubColdAudit, 20)
	writeAppliedProposal(t, store, 20)
	writeColdEntry(t, store, dal.SubColdAuditItem, 20)

	// Above the boundary: post-archival history the restored store must keep.
	writeColdEntry(t, store, dal.SubColdLog, 101)
	writeColdEntry(t, store, dal.SubColdAudit, 41)
	writeAppliedProposal(t, store, 41)

	require.NoError(t, purgeArchivedRanges(ctx, logging.Testing(), store))

	require.Equal(t, 1, countKeysInSub(t, store, dal.SubColdLog),
		"only the log above the archived range survives")
	require.Equal(t, 1, countKeysInSub(t, store, dal.SubColdAudit),
		"only the audit entry above the archived range survives")
	require.Equal(t, 1, countKeysInSub(t, store, dal.SubColdAppliedProposal),
		"the purge receipt inside the archived range is what the exclusion pass was tripping over")

	require.Equal(t, 0, countKeysInSub(t, store, dal.SubColdAuditItem),
		"audit items are purged at the confirm, so the restore must purge them too")
}

// The registry's status is the whole authority for this: a chapter the source had
// not archived still owns its hot entries.
func TestPurgeArchivedRanges_LeavesUnarchivedChaptersAlone(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)
	ctx := context.Background()

	for id, status := range map[uint64]commonpb.ChapterStatus{
		1: commonpb.ChapterStatus_CHAPTER_CLOSED,
		2: commonpb.ChapterStatus_CHAPTER_ARCHIVING,
		3: commonpb.ChapterStatus_CHAPTER_OPEN,
	} {
		writeChapterRow(t, store, &commonpb.Chapter{
			Id:                 id,
			Status:             status,
			StartSequence:      id * 10,
			CloseSequence:      id*10 + 9,
			StartAuditSequence: id * 10,
			CloseAuditSequence: id*10 + 9,
		})

		writeColdEntry(t, store, dal.SubColdLog, id*10+1)
		writeAppliedProposal(t, store, id*10+1)
	}

	require.NoError(t, purgeArchivedRanges(ctx, logging.Testing(), store))

	require.Equal(t, 3, countKeysInSub(t, store, dal.SubColdLog))
	require.Equal(t, 3, countKeysInSub(t, store, dal.SubColdAppliedProposal))
}

// An empty registry must not turn into a range delete over everything.
func TestPurgeArchivedRanges_NoArchivedChaptersIsANoOp(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)
	ctx := context.Background()

	writeColdEntry(t, store, dal.SubColdLog, 7)
	writeAppliedProposal(t, store, 7)

	require.NoError(t, purgeArchivedRanges(ctx, logging.Testing(), store))

	require.Equal(t, 1, countKeysInSub(t, store, dal.SubColdLog))
	require.Equal(t, 1, countKeysInSub(t, store, dal.SubColdAppliedProposal))
}

// The unit tests above cover the purge in isolation; this one covers the
// composition the incremental restore contract asks for — a chapter archived
// inside the exported delta, restored through ApplyExportsAndRebuild, with the
// registry rebuilt from the lifecycle logs and the ranges those logs describe
// gone from hot storage afterwards.
func TestApplyExportsAndRebuild_PurgesTheArchivedRangeItReIngested(t *testing.T) {
	t.Parallel()

	const bucketID = "purge-bucket"

	ctx := context.Background()
	source := newBackupTestStore(t)

	archived := &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      1,
		CloseSequence:      3,
		StartAuditSequence: 1,
		CloseAuditSequence: 2,
		SealingHash:        []byte("sealing-hash"),
		StateHash:          []byte("state-hash"),
		LastAuditHash:      []byte("anchor"),
	}
	opened := &commonpb.Chapter{
		Id:                 2,
		Status:             commonpb.ChapterStatus_CHAPTER_OPEN,
		StartSequence:      4,
		StartAuditSequence: 3,
	}

	closing := archived.CloneVT()
	closing.Status = commonpb.ChapterStatus_CHAPTER_CLOSING
	closing.SealingHash = nil
	closing.StateHash = nil

	batch := source.OpenWriteSession()
	// Chapter 1's own history, plus the lifecycle logs that make the rebuilt
	// registry call it ARCHIVED. The confirm sits above the chapter's close, in
	// the successor, exactly as the FSM emits it.
	require.NoError(t, batch.SetProto(coldAuditKey(1), auditEntryWithHash(1, []byte("h1"))))
	require.NoError(t, batch.SetProto(coldAuditKey(2), auditEntryWithHash(2, []byte("anchor"))))
	require.NoError(t, batch.SetProto(coldLogKey(3), closeChapterLog(3, closing, opened)))
	require.NoError(t, batch.SetProto(coldLogKey(4), archiveChapterLog(4, archived)))
	require.NoError(t, batch.SetProto(coldLogKey(5), confirmArchiveChapterLog(5, archived)))
	require.NoError(t, batch.SetProto(coldAuditKey(3), auditEntryWithHash(3, []byte("h3"))))
	require.NoError(t, batch.Commit())

	// A receipt inside the archived audit range — the row the exclusion pass
	// compares against a replay that skips this range.
	writeAppliedProposal(t, source, 2)
	// And one above it, which must survive.
	writeAppliedProposal(t, source, 3)

	storage := newInMemoryBackupStorage()
	// The full checkpoint stops before the close, so every chapter transition —
	// and the receipt inside the chapter — travels in the delta.
	require.NoError(t, WriteManifest(ctx, storage, ManifestKey(bucketID), &Manifest{
		Checkpoint: &CheckpointManifest{LastLogSequence: 2, LastAuditSequence: 1},
	}))

	result, err := RunIncrementalBackup(ctx, logging.Testing(), source, nil, storage, bucketID, 0)
	require.NoError(t, err)
	require.NotZero(t, result.LogEntriesExported, "the delta must carry the chapter's lifecycle logs")

	manifest, err := ReadManifest(ctx, storage, ManifestKey(bucketID))
	require.NoError(t, err)

	restored := newBackupTestStore(t)
	require.NoError(t, ApplyExportsAndRebuild(ctx, logging.Testing(), storage, restored, manifest))

	rows := readChapterRows(t, restored)
	require.NotNil(t, rows[1])
	require.Equal(t, commonpb.ChapterStatus_CHAPTER_ARCHIVED, rows[1].GetStatus(),
		"the rebuild must call the chapter ARCHIVED, which is what authorises the purge")

	require.Equal(t, 1, countKeysInSub(t, restored, dal.SubColdAppliedProposal),
		"the receipt inside the archived range is gone; the one above it stays")

	logs := readLogSequences(t, restored, 0)
	require.NotContains(t, logs, uint64(3), "log 3 is inside the archived range")
	require.Contains(t, logs, uint64(4), "log 4 is above it and must survive")
	require.Contains(t, logs, uint64(5))

	// The scan must not leave an iterator pinned. FinalizeRestore closes the
	// staging store right after this path, and Pebble takes a still-referenced
	// iterator as grounds to panic in Close — which fails the whole restore, not
	// just the purge. Tests that close with `_ = store.Close()` cannot see it.
	require.NoError(t, restored.Close(), "the restored store must close cleanly after the purge")
}

// A purge that fails must fail the restore. Finalizing after an incomplete purge
// would leave a store whose registry calls a chapter ARCHIVED while its ranges are
// still resident — the state verifyArchivedChapterResidency reports as corrupt,
// reached silently.
func TestApplyExportsAndRebuild_PropagatesAPurgeFailure(t *testing.T) {
	t.Parallel()

	const bucketID = "purge-failure-bucket"

	ctx := context.Background()
	source := newBackupTestStore(t)

	batch := source.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldAuditKey(1), auditEntryWithHash(1, []byte("h1"))))
	require.NoError(t, batch.SetProto(coldLogKey(2), createLedgerLog(2, "l", 1)))
	require.NoError(t, batch.Commit())

	storage := newInMemoryBackupStorage()
	require.NoError(t, WriteManifest(ctx, storage, ManifestKey(bucketID), &Manifest{
		Checkpoint: &CheckpointManifest{LastLogSequence: 1, LastAuditSequence: 0},
	}))

	_, err := RunIncrementalBackup(ctx, logging.Testing(), source, nil, storage, bucketID, 0)
	require.NoError(t, err)

	manifest, err := ReadManifest(ctx, storage, ManifestKey(bucketID))
	require.NoError(t, err)

	restored := newBackupTestStore(t)

	// A chapter row the registry scan cannot decode. The rebuild writes rows for
	// the chapters its logs describe and never reads this id, so the purge's scan
	// is the first step that touches it.
	corrupt := restored.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneGlobal, dal.SubGlobChapters).PutUint64(99).Build()
	require.NoError(t, corrupt.SetBytes(key, []byte("not-a-chapter")))
	require.NoError(t, corrupt.Commit())

	err = ApplyExportsAndRebuild(ctx, logging.Testing(), storage, restored, manifest)
	require.ErrorContains(t, err, "purging archived ranges",
		"the restore must fail rather than finalize after an incomplete purge")
}

// A chapter that closed with no audit entries of its own carries
// close_audit_sequence < start_audit_sequence. The confirm's purge skips the
// audit-keyed ranges for it rather than deleting a range that runs backwards, and
// so must this: a backwards range would either delete nothing or, if the bounds
// were swapped, take the successor's entries with it.
func TestPurgeArchivedRanges_SkipsAnInvertedAuditRange(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)
	ctx := context.Background()

	writeChapterRow(t, store, &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      1,
		CloseSequence:      10,
		StartAuditSequence: 5,
		CloseAuditSequence: 4,
	})

	writeColdEntry(t, store, dal.SubColdLog, 3)
	writeColdEntry(t, store, dal.SubColdAudit, 5)
	writeColdEntry(t, store, dal.SubColdAuditItem, 5)
	writeAppliedProposal(t, store, 5)

	require.NoError(t, purgeArchivedRanges(ctx, logging.Testing(), store))

	require.Equal(t, 0, countKeysInSub(t, store, dal.SubColdLog),
		"the log range is still well-formed and is purged")
	require.Equal(t, 1, countKeysInSub(t, store, dal.SubColdAudit),
		"the audit-keyed ranges are skipped, so the successor's entry at 5 survives")
	require.Equal(t, 1, countKeysInSub(t, store, dal.SubColdAuditItem))
	require.Equal(t, 1, countKeysInSub(t, store, dal.SubColdAppliedProposal))
}

// The registry scan is the first thing the purge does, and a store that cannot be
// read is a restore that must fail rather than finalize.
func TestPurgeArchivedRanges_PropagatesAReadFailure(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)
	ctx := context.Background()

	writeChapterRow(t, store, &commonpb.Chapter{
		Id:     1,
		Status: commonpb.ChapterStatus_CHAPTER_ARCHIVED,
	})

	require.NoError(t, store.Close())

	require.Error(t, purgeArchivedRanges(ctx, logging.Testing(), store))
}

// failingDeleter fails the nth DeleteRange, and optionally the commit, so each
// error return is reachable: on a healthy Pebble batch none of them are.
type failingDeleter struct {
	failOn     int
	failCommit bool
	calls      int
}

func (d *failingDeleter) DeleteRange(_, _ []byte, _ *pebble.WriteOptions) error {
	d.calls++
	if d.calls == d.failOn {
		return errors.New("delete range failed")
	}

	return nil
}

func (d *failingDeleter) Commit() error {
	if d.failCommit {
		return errors.New("commit failed")
	}

	return nil
}

// Each range is deleted in its own call, and a failure in any of them must surface
// rather than leave the chapter half-purged and the restore reporting success.
func TestDeleteChapterRange_SurfacesEachRangeFailure(t *testing.T) {
	t.Parallel()

	chapter := &commonpb.Chapter{
		Id:                 1,
		StartSequence:      1,
		CloseSequence:      10,
		StartAuditSequence: 1,
		CloseAuditSequence: 4,
	}

	for name, tc := range map[string]struct {
		failOn int
		expect string
	}{
		"logs":              {failOn: 1, expect: "purging logs"},
		"audit entries":     {failOn: 2, expect: "purging audit"},
		"audit items":       {failOn: 3, expect: "purging audit items"},
		"applied proposals": {failOn: 4, expect: "purging applied proposals"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := deleteChapterRange(&failingDeleter{failOn: tc.failOn}, chapter)
			require.ErrorContains(t, err, tc.expect)
		})
	}
}

// A commit that fails must fail the restore: the ranges are deleted in one batch,
// so a lost commit leaves every chapter unpurged rather than some.
func TestPurgeChapters_SurfacesACommitFailure(t *testing.T) {
	t.Parallel()

	archived := []*commonpb.Chapter{{
		Id:                 1,
		StartSequence:      1,
		CloseSequence:      10,
		StartAuditSequence: 1,
		CloseAuditSequence: 4,
	}}

	err := purgeChapters(logging.Testing(), &failingDeleter{failCommit: true}, archived)
	require.ErrorContains(t, err, "committing archived range purge")
}

// And a chapter whose delete fails names that chapter, so an operator reading the
// failed restore knows which one stopped it.
func TestPurgeChapters_NamesTheChapterThatFailed(t *testing.T) {
	t.Parallel()

	archived := []*commonpb.Chapter{
		{Id: 7, StartSequence: 1, CloseSequence: 10, StartAuditSequence: 1, CloseAuditSequence: 4},
		{Id: 8, StartSequence: 11, CloseSequence: 20, StartAuditSequence: 5, CloseAuditSequence: 9},
	}

	// Fifth call: chapter 7 takes four, so chapter 8's log delete is the failure.
	err := purgeChapters(logging.Testing(), &failingDeleter{failOn: 5}, archived)
	require.ErrorContains(t, err, "purging archived chapter 8")
	require.ErrorContains(t, err, "purging logs")
}
