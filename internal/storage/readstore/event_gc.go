package readstore

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/pebble/v2"
)

// GCEventZone reclaims superseded events below the read-lease watermark
// across one event zone (the metadata index 0x01 or the exists index 0x02),
// walking at most budget keys from resume (nil = zone start) and stopping
// only at group boundaries. It returns the number of reclaimed events and
// the cursor to resume from on the next call — nil once the zone walk
// completed.
//
// The pruning rule per event group: every event with seq < watermark is
// droppable EXCEPT the latest one, and that one survives only as an ADD.
// Justification: any live or future reader resolves at some pin P >=
// watermark (live pins are lease-registered and bound the watermark; future
// pins are at least the fold cursor, which also bounds it), and the latest
// event <= P is what decides —
//
//   - events superseded below the watermark can never be "latest <= P" again;
//   - a surviving latest-below-watermark ADD still decides P's verdict when
//     the group has no event in (watermark, P];
//   - a latest-below-watermark DEL decides the same verdict as no event at
//     all ("not matching"), so the whole dead group is reclaimed.
func GCEventZone(db *pebble.DB, zone byte, resume []byte, watermark uint64, budget int) (pruned int, next []byte, err error) {
	zonePrefix := []byte{zone}

	lower := zonePrefix
	if len(resume) > 0 && resume[0] == zone {
		lower = resume
	}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: IncrementBytes(zonePrefix),
	})
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = iter.Close() }()

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	suffix := metadataEventSuffixLen + 1 // terminator + seq + op

	var (
		group        []byte // current group identity (key bytes before the terminator)
		pendingBelow []byte // latest below-watermark key seen, not yet judged
		pendingIsAdd bool
		scanned      int
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
		tpos := len(key) - suffix
		if tpos < 1 || key[tpos] != metadataEventTerminator {
			scanned++

			continue // malformed keys are the checker's business, not GC's
		}

		identity := key[:tpos]
		seq := binary.BigEndian.Uint64(key[tpos+1 : tpos+9])
		op := key[tpos+9]

		if !bytes.Equal(identity, group) {
			flush()

			// Budget is only enforced at group boundaries so a group is
			// never judged from a partial view of its events.
			if scanned >= budget {
				next = append([]byte(nil), key...)

				break
			}

			group = append(group[:0], identity...)
		}

		scanned++

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

	if next == nil {
		flush()
	}

	if pruned > 0 {
		if err := batch.Commit(pebble.NoSync); err != nil {
			return 0, nil, err
		}
	}

	return pruned, next, nil
}
