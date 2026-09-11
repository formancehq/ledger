package readstore

import (
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Event-suffixed metadata index keys (EN-1748).
//
// The metadata value index is stored as an append-only event log so that a
// reader pinned at an applied sequence reconstructs the exact membership at
// that sequence from any snapshot taken at or past it. Every mutation
// appends an event key
//
//	[prefix][encodedValue][entity]\x00[raftSeq BE 8][op 1]
//
// where op is ADD or DEL. Two invariants make reads exact at any pinned
// sequence P:
//
//  1. Every transition AWAY from a value deposits a DEL event in THAT value's
//     own key range. The question "does entity E match value V at P?" is
//     answered entirely inside (V, E)'s event group — the latest event with
//     seq <= P decides (ADD: yes; DEL or none: no). Ranges never consult each
//     other, so iteration stays a plain ordered scan.
//  2. Events are immutable once written. A reader that pinned P before a later
//     event was appended simply ignores it (seq > P); a background GC reclaims
//     superseded events only below the read-lease watermark (see read_lease.go
//     and event_gc.go).
//
// The \x00 terminator after the entity is load-bearing: entities are
// variable-length, and without a terminator the seq bytes of entity "a" would
// interleave with the events of entity "ab". Entity IDs cannot contain NUL
// (account addresses are [a-zA-Z0-9:_-]+; transaction ids are their fixed
// 8-byte encoding, terminated uniformly for one parse rule).
const (
	// MetadataEventDel orders before MetadataEventAdd so that an ADD and a
	// DEL carrying the same sequence (impossible today — one log cannot both
	// add and remove the same (value, entity) — but cheap to make total)
	// resolve to ADD.
	MetadataEventDel byte = 0x00
	MetadataEventAdd byte = 0x01

	// metadataEventSuffixLen is the fixed tail after the entity terminator:
	// 8 bytes of big-endian raft sequence plus the op byte.
	metadataEventSuffixLen = 9

	// metadataEventTerminator separates the variable-length entity from the
	// fixed event suffix.
	metadataEventTerminator byte = 0x00
)

// validEventOp reports whether op is one this package writes. Anything else in
// that byte is a corrupt or foreign key: readers must refuse it rather than
// fold it into the DEL arm of an `op == MetadataEventAdd` test, which would
// silently drop matching entities.
func validEventOp(op byte) bool {
	return op == MetadataEventAdd || op == MetadataEventDel
}

// MetadataIndexEventKeyV builds one event key. Layout mirrors
// MetadataIndexKeyV with the entity terminated and the (seq, op) suffix
// appended.
func MetadataIndexEventKeyV(
	kb *dal.KeyBuilder,
	ledgerName string,
	ns, metadataKey string,
	version uint32,
	encodedValue []byte,
	entityID []byte,
	seq uint64,
	op byte,
) []byte {
	return kb.Reset().
		PutByte(PrefixMetadataIndex).
		PutLedgerNameFixed(ledgerName).
		PutNamespace(ns).
		PutStringNull(metadataKey).
		PutUint32(version).
		PutBytes(encodedValue).
		PutBytes(entityID).
		PutByte(metadataEventTerminator).
		PutUint64(seq).
		PutByte(op).
		Consume()
}

// EntityExistsEventKeyV builds one existence event key. Layout mirrors
// EntityExistsKeyV with the entity terminated and the (seq, op) suffix
// appended; the nullFlag byte plays the encoded-value role, so an entity
// moving between null and non-null tombstones the flag range it leaves.
func EntityExistsEventKeyV(
	kb *dal.KeyBuilder,
	ledgerName string,
	ns, metaKey string,
	version uint32,
	isNull bool,
	entityID []byte,
	seq uint64,
	op byte,
) []byte {
	nullFlag := EntityExistsNonNull
	if isNull {
		nullFlag = EntityExistsNull
	}

	return kb.Reset().
		PutByte(PrefixEntityExists).
		PutLedgerNameFixed(ledgerName).
		PutNamespace(ns).
		PutStringNull(metaKey).
		PutUint32(version).
		PutByte(nullFlag).
		PutBytes(entityID).
		PutByte(metadataEventTerminator).
		PutUint64(seq).
		PutByte(op).
		Consume()
}
