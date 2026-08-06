package readstore

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/pebble/v2"
)

// GCMetadataEvents reclaims superseded events below the read-lease watermark
// in one value range (see event_keys.go and read_lease.go).
//
// The pruning rule per (value, entity) group: every event with seq <
// watermark is droppable EXCEPT the latest one, and that one only survives if
// it is an ADD. Justification: any live or future reader resolves at some
// pin P >= watermark, and the latest event <= P is what decides —
//
//   - events superseded below the watermark can never be "latest <= P" again;
//   - a surviving latest-below-watermark ADD still decides P's verdict when
//     the group has no event in (watermark, P];
//   - a latest-below-watermark DEL decides the same verdict as no event at
//     all ("not matching"), so the whole dead pair is reclaimed.
//
// Operates on one value prefix per call.
//
// TODO(EN-1748): incremental whole-zone walk under the builder's
// background-task budget.
func GCMetadataEvents(db *pebble.DB, prefix []byte, watermark uint64) (pruned int, err error) {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: IncrementBytes(prefix),
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	suffix := metadataEventSuffixLen + 1 // terminator + seq + op

	var (
		group        []byte // current entity
		pendingBelow []byte // latest below-watermark key seen, not yet judged
		pendingIsAdd bool
	)

	flush := func() {
		// The group ended: the pending latest-below-watermark event survives
		// only as a live ADD.
		if pendingBelow != nil && !pendingIsAdd {
			_ = batch.Delete(pendingBelow, nil)
			pruned++
		}
		pendingBelow = nil
	}

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		rest := key[len(prefix):]
		tpos := len(rest) - suffix
		if tpos < 0 || rest[tpos] != metadataEventTerminator {
			continue // malformed keys are the checker's business, not GC's
		}

		entity := rest[:tpos]
		seq := binary.BigEndian.Uint64(rest[tpos+1 : tpos+9])
		op := rest[tpos+9]

		if !bytes.Equal(entity, group) {
			flush()
			group = append(group[:0], entity...)
		}

		if seq >= watermark {
			// Above the watermark nothing is reclaimable, and the group's
			// pending below-watermark event gets judged now: a pending ADD
			// still decides pins in [watermark, seq) and survives; a pending
			// DEL decides "dead", the same verdict as absence, and goes.
			flush()

			continue
		}

		// A newer below-watermark event supersedes the previous pending one.
		if pendingBelow != nil {
			_ = batch.Delete(pendingBelow, nil)
			pruned++
		}

		pendingBelow = append(pendingBelow[:0], key...)
		pendingIsAdd = op == MetadataEventAdd
	}

	flush()

	if pruned > 0 {
		if err := batch.Commit(pebble.NoSync); err != nil {
			return 0, err
		}
	}

	return pruned, nil
}
