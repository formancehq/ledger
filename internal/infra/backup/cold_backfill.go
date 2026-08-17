package backup

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source cold_backfill.go -destination cold_backfill_generated_test.go -package backup . ColdChapterReader

// ColdChapterReader yields a Pebble reader over one archived chapter's cold
// SST. Implemented by *coldstorage.ColdReader. A nil reader is valid on
// deployments without cold storage; backfillArchivedRanges then fails loudly
// if an export window overlaps an archived chapter.
type ColdChapterReader interface {
	AcquireReader(ctx context.Context, chapterID uint64) (dal.PebbleReader, func() error, error)
}

// archivedOverlap is the intersection of one ARCHIVED chapter's sequence
// ranges with the incremental export windows. Bounds are inclusive; an empty
// intersection has lo > hi.
type archivedOverlap struct {
	chapter          *commonpb.Chapter
	logLo, logHi     uint64
	auditLo, auditHi uint64
}

// backfillArchivedRanges exports, from cold storage, every part of the
// incremental export windows that chapter archival purged from the hot store.
//
// Confirming a chapter's archival range-deletes its log and audit entries
// from hot Pebble, so a hot-only export silently skips them and the manifest
// floor advances past them — they drop out of the backup chain permanently
// and the eventual restore is corrupt (EN-1598). The chapter registry
// (ZoneGlobal/SubGlobChapters, never purged) names every purged range, and
// the chapter's cold SST holds the purged KV pairs under their original keys,
// so each overlap is re-exported through the same exportEntries pipeline as
// hot data and restores through ApplyExports unchanged.
//
// hotReader must be the same snapshot the caller exports hot entries from: a
// chapter is ARCHIVED in that snapshot if and only if its purge is applied in
// it, so cold and hot segments can never overlap.
//
// The returned segments must precede the hot segments in Manifest.Exports:
// LastExportLogSequence / LastExportAuditSequence take the LAST segment of
// each type as the next run's floor, and the hot tail always carries the
// highest sequences (the ConfirmArchiveChapter log itself lands above the
// chapter it archives).
func backfillArchivedRanges(
	ctx context.Context,
	logger logging.Logger,
	storage Storage,
	hotReader dal.PebbleReader,
	coldReader ColdChapterReader,
	bucketID string,
	afterLogSeq, currentLogSeq uint64,
	afterAuditSeq, currentAuditSeq uint64,
	maxSegmentBytes int64,
) (segments []ExportSegment, logCount, auditCount uint64, err error) {
	overlaps, err := archivedOverlaps(ctx, hotReader, afterLogSeq, currentLogSeq, afterAuditSeq, currentAuditSeq)
	if err != nil {
		return nil, 0, 0, err
	}

	if len(overlaps) == 0 {
		return nil, 0, 0, nil
	}

	// The gap is detected — an archived chapter overlaps the export window —
	// but there is no cold storage to read it back from. Advancing the cursor
	// anyway would silently drop the range from the backup chain forever, so
	// this must fail the backup.
	if coldReader == nil {
		return nil, 0, 0, fmt.Errorf(
			"incremental export window (log (%d, %d], audit (%d, %d]) overlaps archived chapter %d whose entries are purged from hot storage, and no cold storage is configured to backfill them from",
			afterLogSeq, currentLogSeq, afterAuditSeq, currentAuditSeq, overlaps[0].chapter.GetId())
	}

	for _, ov := range overlaps {
		chapterID := ov.chapter.GetId()

		logger.WithFields(map[string]any{
			"chapterId": chapterID,
			"logRange":  fmt.Sprintf("[%d, %d]", ov.logLo, ov.logHi),
		}).Infof("Backfilling archived chapter range from cold storage")

		cold, release, err := coldReader.AcquireReader(ctx, chapterID)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("opening cold archive for chapter %d: %w", chapterID, err)
		}
		if release == nil {
			return nil, 0, 0, fmt.Errorf("invariant: cold archive lease for chapter %d has no release function", chapterID)
		}
		chapterSegments, chapterLogCount, chapterAuditCount, exportErr := backfillArchivedOverlap(
			ctx,
			storage,
			cold,
			bucketID,
			ov,
			maxSegmentBytes,
		)
		releaseErr := release()
		if releaseErr != nil {
			releaseErr = fmt.Errorf("releasing cold archive for chapter %d: %w", chapterID, releaseErr)
		}
		if err := errors.Join(exportErr, releaseErr); err != nil {
			return nil, 0, 0, err
		}
		segments = append(segments, chapterSegments...)
		logCount += chapterLogCount
		auditCount += chapterAuditCount
	}

	return segments, logCount, auditCount, nil
}

func backfillArchivedOverlap(
	ctx context.Context,
	storage Storage,
	cold dal.PebbleReader,
	bucketID string,
	ov archivedOverlap,
	maxSegmentBytes int64,
) (segments []ExportSegment, logCount, auditCount uint64, err error) {
	chapterID := ov.chapter.GetId()
	if ov.logLo <= ov.logHi {
		segs, count, err := exportEntries(
			ctx, storage, cold,
			dal.ZoneCold, dal.SubColdLog, ov.logLo-1, ov.logHi, "log",
			func(part int) string { return ExportLogSegmentKey(bucketID, ov.logLo, ov.logHi, part) },
			maxSegmentBytes,
		)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("exporting archived log entries for chapter %d: %w", chapterID, err)
		}

		// The log stream is gap-free (one log per sequence, allocated on
		// append), so the archive must hold exactly the overlap. Fewer
		// means the SST is truncated or the chapter registry is wrong —
		// exporting it would publish a backup missing entries the purge
		// already deleted from hot.
		if expected := ov.logHi - ov.logLo + 1; count != expected {
			return nil, 0, 0, fmt.Errorf(
				"invariant: cold archive for chapter %d holds %d log entries in [%d, %d], expected %d",
				chapterID, count, ov.logLo, ov.logHi, expected)
		}

		segments = append(segments, segs...)
		logCount += count
	}

	if ov.auditLo <= ov.auditHi {
		segs, count, err := exportEntries(
			ctx, storage, cold,
			dal.ZoneCold, dal.SubColdAudit, ov.auditLo-1, ov.auditHi, "audit",
			func(part int) string { return ExportAuditSegmentKey(bucketID, ov.auditLo, ov.auditHi, part) },
			maxSegmentBytes,
		)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("exporting archived audit entries for chapter %d: %w", chapterID, err)
		}

		// The audit stream is a hash chain — one entry per sequence, no
		// gaps — so the same completeness bound applies as for logs.
		if expected := ov.auditHi - ov.auditLo + 1; count != expected {
			return nil, 0, 0, fmt.Errorf(
				"invariant: cold archive for chapter %d holds %d audit entries in [%d, %d], expected %d",
				chapterID, count, ov.auditLo, ov.auditHi, expected)
		}

		segments = append(segments, segs...)
		auditCount += count

		// Audit items and applied proposals share the audit sequence
		// counter but are legitimately sparse (failures carry no items,
		// only successes write an AppliedProposal), so no completeness
		// bound applies and empty results add no segment — same contract
		// as the hot export.
		itemSegs, _, err := exportEntries(
			ctx, storage, cold,
			dal.ZoneCold, dal.SubColdAuditItem, ov.auditLo-1, ov.auditHi, "auditItem",
			func(part int) string { return ExportAuditItemSegmentKey(bucketID, ov.auditLo, ov.auditHi, part) },
			maxSegmentBytes,
		)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("exporting archived audit items for chapter %d: %w", chapterID, err)
		}

		segments = append(segments, itemSegs...)

		appliedSegs, _, err := exportEntries(
			ctx, storage, cold,
			dal.ZoneCold, dal.SubColdAppliedProposal, ov.auditLo-1, ov.auditHi, "appliedProposal",
			func(part int) string { return ExportAppliedProposalSegmentKey(bucketID, ov.auditLo, ov.auditHi, part) },
			maxSegmentBytes,
		)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("exporting archived applied proposals for chapter %d: %w", chapterID, err)
		}

		segments = append(segments, appliedSegs...)
	}

	return segments, logCount, auditCount, nil
}

// archivedOverlaps returns, ordered by chapter start sequence, every ARCHIVED
// chapter whose log range intersects (afterLogSeq, currentLogSeq] or whose
// audit range intersects (afterAuditSeq, currentAuditSeq]. Only ARCHIVED
// chapters qualify: the purge is applied atomically with that status, so any
// other status means the chapter's entries are still in hot storage and the
// regular hot export covers them.
func archivedOverlaps(
	ctx context.Context,
	reader dal.PebbleReader,
	afterLogSeq, currentLogSeq uint64,
	afterAuditSeq, currentAuditSeq uint64,
) ([]archivedOverlap, error) {
	cursor, err := query.ReadChapters(ctx, reader)
	if err != nil {
		return nil, fmt.Errorf("reading chapters: %w", err)
	}

	defer func() { _ = cursor.Close() }()

	var overlaps []archivedOverlap

	for {
		chapter, err := cursor.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("iterating chapters: %w", err)
		}

		if chapter.GetStatus() != commonpb.ChapterStatus_CHAPTER_ARCHIVED {
			continue
		}

		ov := archivedOverlap{
			chapter: chapter,
			logLo:   max(chapter.GetStartSequence(), afterLogSeq+1),
			logHi:   min(chapter.GetCloseSequence(), currentLogSeq),
			auditLo: max(chapter.GetStartAuditSequence(), afterAuditSeq+1),
			auditHi: min(chapter.GetCloseAuditSequence(), currentAuditSeq),
		}

		if ov.logLo > ov.logHi && ov.auditLo > ov.auditHi {
			continue
		}

		overlaps = append(overlaps, ov)
	}

	// Chapters can be archived out of order, so registry order (by chapter id)
	// does not imply sequence order.
	slices.SortFunc(overlaps, func(a, b archivedOverlap) int {
		return cmp.Compare(a.chapter.GetStartSequence(), b.chapter.GetStartSequence())
	})

	return overlaps, nil
}
