package check

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// verifyArchivedChapterResidency reports hot-store keys resident inside an
// ARCHIVED chapter's purge ranges. ConfirmArchiveChapter's apply deletes the
// chapter's logs by log sequence and its audit entries, audit items and
// applied proposals by audit sequence, in the same proposal that marks the
// registry row ARCHIVED — so a resident key under an ARCHIVED row means the
// purge was lost, or a path re-ingested archived history without purging it
// (e.g. a restore rebuilt from a cold-backfilled delta). The canonical copy
// lives in the chapter's cold-storage object; hot residue shadows it on read
// paths that try hot storage first and keeps the store from ever shrinking
// for the archived range.
//
// One event is emitted per (chapter, sub-prefix) with a resident-key count.
// Chapters are walked in registry order and the sub-prefixes in fixed order,
// so the event stream is deterministic and bounded by the chapter count
// rather than by the residue size.
func verifyArchivedChapterResidency(reader dal.PebbleReader, chapters []*commonpb.Chapter, callback func(*servicepb.CheckStoreEvent)) error {
	for _, ch := range chapters {
		if ch.GetStatus() != commonpb.ChapterStatus_CHAPTER_ARCHIVED {
			continue
		}

		ranges := []struct {
			name   string
			sub    byte
			lo, hi uint64
		}{
			{"log", dal.SubColdLog, ch.GetStartSequence(), ch.GetCloseSequence()},
			{"audit entry", dal.SubColdAudit, ch.GetStartAuditSequence(), ch.GetCloseAuditSequence()},
			{"audit item", dal.SubColdAuditItem, ch.GetStartAuditSequence(), ch.GetCloseAuditSequence()},
			{"applied proposal", dal.SubColdAppliedProposal, ch.GetStartAuditSequence(), ch.GetCloseAuditSequence()},
		}

		for _, r := range ranges {
			// A chapter with no entries of a kind carries close < start for
			// that range; the purge skips those ranges and so does the scan.
			if r.hi < r.lo {
				continue
			}

			count, firstSeq, err := countResidentKeys(reader, r.sub, r.lo, r.hi)
			if err != nil {
				return fmt.Errorf("scanning %s residency for chapter %d: %w", r.name, ch.GetId(), err)
			}

			if count == 0 {
				continue
			}

			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_UNPURGED_ARCHIVED_DATA,
				fmt.Sprintf("archived chapter %d holds %d unpurged %s key(s) in hot storage over [%d, %d], first at sequence %d",
					ch.GetId(), count, r.name, r.lo, r.hi, firstSeq),
				ch.GetCloseSequence(), "", "", ""))
		}
	}

	return nil
}

// countResidentKeys counts hot-store keys under {ZoneCold, sub} whose 8-byte
// big-endian sequence lies in [lo, hi], returning the count and the sequence
// of the first resident key. The upper bound is sequence-only, so keys with a
// suffix after the sequence (audit items carry a 4-byte order index) fall
// inside the range for every sequence up to and including hi.
func countResidentKeys(reader dal.PebbleReader, sub byte, lo, hi uint64) (int, uint64, error) {
	lower := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, sub).PutUint64(lo).Build()
	upper := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, sub).PutUint64(hi + 1).Build()

	iter, err := reader.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0, 0, fmt.Errorf("creating iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	var (
		count    int
		firstSeq uint64
	)

	for iter.First(); iter.Valid(); iter.Next() {
		if count == 0 && len(iter.Key()) >= 10 {
			firstSeq = binary.BigEndian.Uint64(iter.Key()[2:10])
		}

		count++
	}

	return count, firstSeq, iter.Error()
}
