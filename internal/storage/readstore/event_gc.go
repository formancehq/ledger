package readstore

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

type eventGCIterator interface {
	First() bool
	Valid() bool
	Next() bool
	Key() []byte
	Value() []byte
	Error() error
}

// GCEventZone reclaims superseded events below the read-lease watermark
// across one event zone (metadata 0x01, exists 0x02, or account asset 0x0C),
// processing at most max(budget, 2) keys from resume (nil = zone start), with
// at most one lookahead key to detect a group boundary. A slice that
// prunes part of a group resumes from that group's start: the surviving
// below-watermark event is its durable progress marker. It returns the number
// of changed index rows
// (reclaimed events plus converted RC.5 rows) and the cursor for the next
// call. A nil cursor means the zone walk completed. In 0x0C it also replaces
// RC.5 first-touch rows with equivalent ADD events in an atomic batch. A
// group being converted is not pruned in the same pass because its old key
// sorts before all its events regardless of seq.
//
// The pruning rule per event group: a below-watermark event is droppable as
// soon as a later below-watermark event in that group is observed. The latest
// one survives as an ADD; a final DEL is removed at the group boundary.
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
func GCEventZone(db *pebble.DB, zone byte, resume []byte, watermark uint64, budget int) (changed int, next []byte, err error) {
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

	return gcEventZoneWithIterator(db, iter, watermark, budget)
}

func gcEventZoneWithIterator(db *pebble.DB, iter eventGCIterator, watermark uint64, budget int) (changed int, next []byte, err error) {
	if budget < 1 {
		return 0, nil, fmt.Errorf("event GC budget must be positive: %d", budget)
	}
	// Comparing a candidate with its successor requires two rows. The
	// production budget is larger; one-row callers get the minimum useful slice.
	if budget == 1 {
		budget = 2
	}

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	var (
		group        []byte // full key identity, excluding the event terminator
		pendingBelow []byte // latest below-watermark event retained as the anchor
		pendingOp    byte
		groupChanged bool
		groupScanned int
		scanned      int
	)

	// A final DEL may be removed only after all preceding events were visited
	// and queued for deletion in this pass (or removed by earlier slices).
	closeGroup := func() error {
		if pendingBelow != nil && pendingOp == IndexEventDel {
			if err := batch.Delete(pendingBelow, nil); err != nil {
				return err
			}
			changed++
		}
		pendingBelow = nil
		groupChanged = false

		return nil
	}

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		prefixLen := 1
		// TODO: Remove legacy detection, Value() from eventGCIterator, and
		// the conversion branch below when no supported live read store or
		// restore path can contain RC.5 plain account-by-asset rows.
		if len(key) > 0 && key[0] == PrefixAccountByAsset {
			var ok bool
			prefixLen, ok = accountAssetPrefixLen(key)
			if !ok {
				return 0, nil, fmt.Errorf("malformed account-asset event GC key %x", key)
			}
		}
		row, ok := parseIndexEventRow(iter, prefixLen)
		if !ok {
			return 0, nil, fmt.Errorf("malformed event GC key %x", key)
		}

		identity := key
		if !row.legacy {
			identity = key[:len(key)-indexEventSuffixLen-1]
		}
		if !bytes.Equal(identity, group) {
			if err := closeGroup(); err != nil {
				return 0, nil, err
			}
			if scanned >= budget {
				next = append([]byte(nil), key...)

				break
			}
			group = append(group[:0], identity...)
			groupScanned = 0
		}

		scanned++
		groupScanned++
		if row.legacy {
			// A legacy stamp is a synthetic ADD. Rewrite it atomically and
			// revisit the group before pruning: its plain key sorts before
			// events regardless of the stamp's sequence.
			converted := make([]byte, 0, len(key)+10)
			converted = append(converted, key...)
			converted = append(converted, indexEventTerminator)
			converted = binary.BigEndian.AppendUint64(converted, row.seq)
			converted = append(converted, IndexEventAdd)
			if err := batch.Set(converted, nil, nil); err != nil {
				return 0, nil, err
			}
			if err := batch.Delete(key, nil); err != nil {
				return 0, nil, err
			}
			changed++ // one legacy row replaced
			next = append([]byte(nil), group...)

			break
		}

		if row.seq < watermark {
			// The later event decides every admissible pin in place of the
			// previous one. Keep only the latest seen below the watermark.
			if pendingBelow != nil {
				if err := batch.Delete(pendingBelow, nil); err != nil {
					return 0, nil, err
				}
				changed++
				groupChanged = true
			}
			pendingBelow = append(pendingBelow[:0], key...)
			pendingOp = row.op
		}

		if scanned >= budget {
			switch {
			case groupChanged:
				// Re-read the group after committing its shortened prefix. An
				// older event inserted by backfill between slices is then seen.
				next = append([]byte(nil), group...)
			case groupScanned > 1 || pendingBelow == nil:
				// No earlier event can be reclaimed from this group: after
				// the first event at/above the watermark all later ones are
				// above it too. Reclaim a pending DEL, then skip the rest.
				if err := closeGroup(); err != nil {
					return 0, nil, err
				}
				groupPrefix := append(append([]byte(nil), group...), indexEventTerminator)
				next = IncrementBytes(groupPrefix)
			default:
				// The budget landed on the first below-watermark event
				// in this group. Revisit it to compare with its successor.
				next = append([]byte(nil), group...)
			}

			break
		}
	}

	// !Valid means either clean exhaustion or an iterator failure. Settling the
	// group and committing here without distinguishing them would turn a partial
	// traversal into completed coverage and permanently strand unvisited keys.
	if iterErr := iter.Error(); iterErr != nil {
		return 0, nil, iterErr
	}

	if next == nil {
		if err := closeGroup(); err != nil {
			return 0, nil, err
		}
	}

	if changed > 0 {
		if err := batch.Commit(pebble.NoSync); err != nil {
			return 0, nil, err
		}
	}

	return changed, next, nil
}
