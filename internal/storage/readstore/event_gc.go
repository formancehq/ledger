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
		reclaimable  [][]byte // the current group's condemned events, applied at its boundary
		groupUnsafe  bool     // the current group holds an event this package cannot read
		carryUnsafe  bool     // ...and so does the next one, for a key that could belong to either
		unsafeFrom   []byte   // the unattributable key a carried mark came from
		scanned      int
	)

	// settle judges the pending latest-below-watermark event, which survives
	// only as a live ADD. It condemns; it does not reclaim.
	settle := func() {
		if pendingBelow != nil && !pendingIsAdd {
			reclaimable = append(reclaimable, append([]byte(nil), pendingBelow...))
		}

		pendingBelow = nil
	}

	// closeGroup applies what the group condemned. Reclamation is decided per
	// event but applied only here, because whether it is safe at all is not
	// known until the whole group has been read: an event superseded early is
	// condemned long before a later unreadable one proves the group must be
	// preserved whole.
	closeGroup := func() {
		settle()

		if !groupUnsafe {
			for _, key := range reclaimable {
				_ = batch.Delete(key, nil)
				pruned++
			}
		}

		reclaimable = reclaimable[:0]
		groupUnsafe = carryUnsafe
		carryUnsafe = false
	}

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		tpos := len(key) - suffix
		if tpos < 1 || key[tpos] != metadataEventTerminator {
			// The identity cannot be parsed, so which group the key belongs to
			// is unknowable: a truncated key sorts before the very events it
			// shares an identity with, so it lands between two groups and
			// could have come from either. Both are preserved — the one being
			// accumulated and the one that follows. Removing the key itself is
			// the checker's business, not GC's.
			scanned++
			groupUnsafe = true
			carryUnsafe = true
			unsafeFrom = append(unsafeFrom[:0], key...)

			continue
		}

		identity := key[:tpos]

		if !bytes.Equal(identity, group) {
			closeGroup()

			// Budget is only enforced at group boundaries so a group is
			// never judged from a partial view of its events.
			if scanned >= budget {
				resume := key

				// A mark carried into the group starting here lives only in
				// memory. Resuming past the key that raised it would leave the
				// next pass with no reason to preserve this group, so the scan
				// rewinds to that key and re-derives the mark from storage.
				if groupUnsafe && len(unsafeFrom) > 0 {
					resume = unsafeFrom
				}

				next = append([]byte(nil), resume...)

				break
			}

			group = append(group[:0], identity...)
		}

		scanned++

		op := key[tpos+9]
		if !validEventOp(op) {
			// Unreadable: the events around it are the only evidence of what
			// this group holds, and one of them may be the live ADD the
			// unreadable event was meant to supersede. Preserve the group and
			// let the query path reject it loudly.
			groupUnsafe = true

			continue
		}

		seq := binary.BigEndian.Uint64(key[tpos+1 : tpos+9])

		if seq >= watermark {
			// Above the watermark nothing is reclaimable, and the group's
			// pending below-watermark event gets judged now: a pending ADD
			// still decides pins in [watermark, seq) and survives; a pending
			// DEL decides "dead", the same verdict as absence, and is
			// condemned. Judging is not reclaiming — the group may still turn
			// out to hold an event this package cannot read.
			settle()

			continue
		}

		// A newer below-watermark event supersedes the previous pending one.
		if pendingBelow != nil {
			reclaimable = append(reclaimable, append([]byte(nil), pendingBelow...))
		}

		pendingBelow = append(pendingBelow[:0], key...)
		pendingIsAdd = op == MetadataEventAdd
	}

	if next == nil {
		closeGroup()
	}

	if pruned > 0 {
		if err := batch.Commit(pebble.NoSync); err != nil {
			return 0, nil, err
		}
	}

	return pruned, next, nil
}
