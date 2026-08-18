package readstore

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Write side of the event-suffixed metadata index (see event_keys.go). The
// builder folds one log at a time and declares its raft sequence via
// SetEventSequence; every index mutation becomes one or two APPENDS, never a
// delete:
//
//	set   k=v   (fresh)   → ADD in v's range
//	set   k=v2  (was v1)  → ADD in v2's range + DEL in v1's range
//	unset k     (was v1)  → DEL in v1's range
//
// The exists index (eidx) gets the same treatment keyed on its nullFlag: an
// event is emitted only when the flag transitions (fresh set, null↔non-null
// move, unset) — a value change within the same flag leaves the standing ADD
// as the group's verdict at every pin.
//
// The old value still comes from the reverse map, exactly like today's
// path — the rmap keeps its role and its prompt deletion semantics (the
// checker's reverse-map pass is untouched).

// eventSequence returns the declared raft sequence for sequence-stamped
// writes — metadata events and the single-stamp rows (account-by-asset,
// reverted_at) — failing loudly when unset: stamping a zero or stale
// sequence would silently corrupt pinned resolution for every future reader.
func (wb *WriteBatch) eventSequence() (uint64, error) {
	if wb.eventSeq == 0 {
		return 0, errors.New("invariant: sequence-stamped index write without SetEventSequence")
	}

	return wb.eventSeq, nil
}

func (wb *WriteBatch) appendMetadataIndexEvent(
	kb *dal.KeyBuilder,
	ledgerName string,
	ns, metadataKey string,
	version uint32,
	encodedValue []byte,
	entityID []byte,
	op byte,
) error {
	seq, err := wb.eventSequence()
	if err != nil {
		return err
	}

	return wb.put(MetadataIndexEventKeyV(kb, ledgerName, ns, metadataKey, version, encodedValue, entityID, seq, op), nil)
}

func (wb *WriteBatch) appendEntityExistsEvent(
	kb *dal.KeyBuilder,
	ledgerName string,
	ns, metaKey string,
	version uint32,
	isNull bool,
	entityID []byte,
	op byte,
) error {
	seq, err := wb.eventSequence()
	if err != nil {
		return err
	}

	return wb.put(EntityExistsEventKeyV(kb, ledgerName, ns, metaKey, version, isNull, entityID, seq, op), nil)
}
