package readstore

import (
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Write side of the event-suffixed metadata index (design sketch — see
// event_keys.go). The builder folds one log at a time and knows the log's
// raft sequence; every index mutation becomes one or two APPENDS, never a
// delete:
//
//	set   k=v   (fresh)   → ADD in v's range
//	set   k=v2  (was v1)  → ADD in v2's range + DEL in v1's range
//	unset k     (was v1)  → DEL in v1's range
//
// The old value still comes from the reverse map, exactly like today's
// delete-old-forward-key path — the rmap keeps its role and its prompt
// deletion semantics (the checker's reverse-map pass is untouched).

// AppendMetadataIndexEvent appends a single ADD or DEL event.
func (wb *WriteBatch) AppendMetadataIndexEvent(
	kb *dal.KeyBuilder,
	ledgerName string,
	ns, metadataKey string,
	version uint32,
	encodedValue []byte,
	entityID []byte,
	seq uint64,
	op byte,
) error {
	return wb.put(MetadataIndexEventKeyV(kb, ledgerName, ns, metadataKey, version, encodedValue, entityID, seq, op), nil)
}

// ReplaceMetadataIndexEvents is the update shape: tombstone the old value's
// range and add to the new one, in the same fold batch — atomic under any
// snapshot. oldEncoded nil means a fresh set (no tombstone); newEncoded nil
// means an unset (no add). Costs the same number of writes as today's
// delete-then-put.
func (wb *WriteBatch) ReplaceMetadataIndexEvents(
	kb *dal.KeyBuilder,
	ledgerName string,
	ns, metadataKey string,
	version uint32,
	oldEncoded, newEncoded []byte,
	entityID []byte,
	seq uint64,
) error {
	if oldEncoded != nil {
		if err := wb.AppendMetadataIndexEvent(kb, ledgerName, ns, metadataKey, version, oldEncoded, entityID, seq, MetadataEventDel); err != nil {
			return err
		}
	}

	if newEncoded != nil {
		return wb.AppendMetadataIndexEvent(kb, ledgerName, ns, metadataKey, version, newEncoded, entityID, seq, MetadataEventAdd)
	}

	return nil
}
