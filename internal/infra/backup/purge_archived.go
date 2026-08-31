package backup

import (
	"context"
	"errors"
	"fmt"
	"io"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// purgeArchivedRanges deletes from hot storage the log and audit ranges of every
// ARCHIVED chapter in the rebuilt registry, so a restored store holds the split
// the source held.
//
// A backup carries those ranges on purpose: an incremental export window that
// overlaps an archived chapter is backfilled from cold storage
// (backfillArchivedRanges), because a delta with holes cannot rebuild derived
// state. RebuildDelta needs them for exactly that. Once it has run they are
// finished: the registry says the chapter is ARCHIVED, which means its canonical
// home is the cold object.
//
// Leaving them resident diverges from the source in three visible ways. Reads
// answer from hot storage for history the registry calls archived
// (ReadLogBySequenceWithCold tries hot first). The disk carries a second copy of
// it. And the checker's exclusion pass finds purge receipts in
// AppliedProposal.TransientVolumes that its replay never derives, because replay
// skips everything at or below the archive boundary — a healthy store reported as
// corrupt, which is the signal telling us the ranges should not be there.
//
// The ranges mirror WriteSet.executePurge, so the result is the split the archival
// confirm produced on the source. Audit items are outside it there and stay
// outside it here.
//
// Not covered: the confirm also runs the deferred cleanup for ledgers whose
// DeleteLedger log falls inside the purged range (State.PendingLedgerCleanups).
// For a ledger deleted before the checkpoint and archived inside the delta, that
// cleanup is neither in the checkpoint nor replayed by the rebuild, so its data
// can outlive the restore. Tracked separately; it needs FSM-side state this path
// does not hold.
func purgeArchivedRanges(ctx context.Context, logger logging.Logger, store *dal.Store) error {
	archived, err := archivedChapters(ctx, store)
	if err != nil {
		return err
	}

	if len(archived) == 0 {
		return nil
	}

	batch := store.OpenWriteSession()

	for _, chapter := range archived {
		if err := deleteChapterRange(batch, chapter); err != nil {
			return fmt.Errorf("purging archived chapter %d: %w", chapter.GetId(), err)
		}

		logger.WithFields(map[string]any{
			"chapterId": chapter.GetId(),
			"logs":      fmt.Sprintf("[%d, %d]", chapter.GetStartSequence(), chapter.GetCloseSequence()),
			"audit":     fmt.Sprintf("[%d, %d]", chapter.GetStartAuditSequence(), chapter.GetCloseAuditSequence()),
		}).Infof("Purging archived chapter range re-ingested by the restore")
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("committing archived range purge: %w", err)
	}

	return nil
}

// archivedChapters reads the rebuilt registry and returns the ARCHIVED rows.
func archivedChapters(ctx context.Context, store *dal.Store) ([]*commonpb.Chapter, error) {
	handle, err := store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	cursor, err := query.ReadChapters(ctx, handle)
	if err != nil {
		return nil, fmt.Errorf("reading chapters: %w", err)
	}

	var archived []*commonpb.Chapter

	for {
		chapter, err := cursor.Next()
		if errors.Is(err, io.EOF) {
			return archived, nil
		}

		if err != nil {
			return nil, fmt.Errorf("reading chapter: %w", err)
		}

		if chapter.GetStatus() == commonpb.ChapterStatus_CHAPTER_ARCHIVED {
			archived = append(archived, chapter)
		}
	}
}

// deleteChapterRange removes one chapter's hot entries. The key ranges are the
// ones WriteSet.executePurge deletes at archival confirmation.
func deleteChapterRange(batch *dal.WriteSession, chapter *commonpb.Chapter) error {
	logStart := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(chapter.GetStartSequence()).Build()
	logEnd := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(chapter.GetCloseSequence() + 1).Build()

	if err := batch.DeleteRange(logStart, logEnd, nil); err != nil {
		return fmt.Errorf("purging logs [%d, %d]: %w", chapter.GetStartSequence(), chapter.GetCloseSequence(), err)
	}

	if chapter.GetCloseAuditSequence() < chapter.GetStartAuditSequence() {
		return nil
	}

	auditStart := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(chapter.GetStartAuditSequence()).Build()
	auditEnd := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(chapter.GetCloseAuditSequence() + 1).Build()

	if err := batch.DeleteRange(auditStart, auditEnd, nil); err != nil {
		return fmt.Errorf("purging audit [%d, %d]: %w", chapter.GetStartAuditSequence(), chapter.GetCloseAuditSequence(), err)
	}

	// AppliedProposal rows share the audit counter. These carry the transient
	// volume receipts the exclusion pass compares against replay.
	proposalStart := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAppliedProposal).PutUint64(chapter.GetStartAuditSequence()).Build()
	proposalEnd := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAppliedProposal).PutUint64(chapter.GetCloseAuditSequence() + 1).Build()

	if err := batch.DeleteRange(proposalStart, proposalEnd, nil); err != nil {
		return fmt.Errorf("purging applied proposals [%d, %d]: %w", chapter.GetStartAuditSequence(), chapter.GetCloseAuditSequence(), err)
	}

	return nil
}
