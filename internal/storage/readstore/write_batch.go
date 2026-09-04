package readstore

import (
	"bytes"
	"encoding/binary"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// WriteBatch buffers Pebble write operations using a dal.WriteSession.
type WriteBatch struct {
	batch *dal.WriteSession
	count int // number of operations buffered

	// eventZones records which append-only event keyspaces this batch actually
	// wrote. The indexbuilder snapshots it immediately before Flush and advances
	// its in-memory GC write epochs only after the commit succeeds.
	eventZones EventZoneMask

	// rmapOverlay is a read-your-writes view of the reverse-map mutations made in
	// the current (uncommitted) batch: reverseKey -> encoded value last written
	// (nil = deleted). It is reset by Init so it always matches the bound batch,
	// and is written by ReplaceMetadataIndex/DeleteMetadataEntryWithPrevious
	// themselves — so a reverse-map write that is not mirrored is unrepresentable.
	// Callers resolve the index's current value for a key via ReverseMapOverlay
	// (uncommitted batch) before falling back to committed state.
	rmapOverlay map[string][]byte

	// eventSeq is the raft sequence stamped onto midx/eidx event keys, set by
	// the caller per folded log (SetEventSequence). Zero means unset — event
	// writes fail loudly rather than stamping a stale or absent sequence.
	eventSeq uint64
}

// EventZoneMask identifies append-only event keyspaces dirtied by a WriteBatch.
// It is intentionally opaque: callers query membership by read-store prefix so
// the bit representation cannot leak into scheduler state.
type EventZoneMask uint8

const (
	eventZoneMetadataIndex EventZoneMask = 1 << iota
	eventZoneEntityExists
)

// Has reports whether the batch appended an event in zone.
func (m EventZoneMask) Has(zone byte) bool {
	switch zone {
	case PrefixMetadataIndex:
		return m&eventZoneMetadataIndex != 0
	case PrefixEntityExists:
		return m&eventZoneEntityExists != 0
	default:
		return false
	}
}

// SetEventSequence declares the raft sequence for subsequent metadata /
// exists index event writes. The fold sets it once per log before
// dispatching; the schema-rewrite backfill sets it to the FSM handle's
// applied sequence for the scan batch.
func (wb *WriteBatch) SetEventSequence(seq uint64) {
	wb.eventSeq = seq
}

// NewWriteBatch creates a new WriteBatch.
func NewWriteBatch() *WriteBatch {
	return &WriteBatch{}
}

// Init binds the batch to a dal.WriteSession and resets the read-your-writes
// reverse-map overlay. This is the only place a batch is bound, so the overlay
// can never be left stale relative to the batch it tracks.
func (wb *WriteBatch) Init(batch *dal.WriteSession) {
	wb.batch = batch
	wb.rmapOverlay = make(map[string][]byte)
	wb.eventSeq = 0
	wb.eventZones = 0
}

// EventZones returns a read-only snapshot of the event keyspaces dirtied by
// successful puts in the current batch.
func (wb *WriteBatch) EventZones() EventZoneMask {
	return wb.eventZones
}

// ReverseMapOverlay returns the encoded value this batch last wrote for
// reverseKey and whether the key was touched in the current batch. A (nil, true)
// result means the key was deleted in this batch; (nil, false) means untouched —
// the caller should consult committed state.
func (wb *WriteBatch) ReverseMapOverlay(reverseKey []byte) ([]byte, bool) {
	v, ok := wb.rmapOverlay[string(reverseKey)]

	return v, ok
}

// Batch returns the underlying dal.WriteSession for direct operations (e.g., range deletes).
func (wb *WriteBatch) Batch() *dal.WriteSession {
	return wb.batch
}

// Empty returns true if no operations have been buffered.
func (wb *WriteBatch) Empty() bool {
	return wb.count == 0
}

// Reset clears the batch state.
func (wb *WriteBatch) Reset() {
	wb.batch = nil
	wb.count = 0
	wb.rmapOverlay = nil
	wb.eventSeq = 0
	wb.eventZones = 0
}

// put sets a key-value pair in the batch.
func (wb *WriteBatch) put(key, value []byte) error {
	if err := wb.batch.SetBytes(key, value); err != nil {
		return err
	}

	wb.count++

	return nil
}

// del deletes a key in the batch.
func (wb *WriteBatch) del(key []byte) error {
	if err := wb.batch.DeleteKey(key); err != nil {
		return err
	}

	wb.count++

	return nil
}

// DeleteReverseMapKey deletes a reverse-map key in the batch and records the
// deletion in the read-your-writes overlay (rmapOverlay[key] = nil), so a
// subsequent same-batch ReverseMapOverlay lookup reports the key as deleted
// rather than surfacing a stale in-flight value. Use this for every reverse-map
// delete so the overlay never drifts from the batch it tracks.
func (wb *WriteBatch) DeleteReverseMapKey(reverseKey []byte) error {
	if err := wb.del(reverseKey); err != nil {
		return err
	}

	wb.rmapOverlay[string(reverseKey)] = nil

	return nil
}

// RangeReverseMapOverlay calls fn for every reverse-map mutation buffered in the
// current (uncommitted) batch: reverseKey -> encoded value last written (nil =
// deleted in this batch). It is a read-only view of the read-your-writes
// overlay — callers that need to delete matching keys must collect them and
// delete after iteration returns, to avoid mutating the overlay mid-range.
func (wb *WriteBatch) RangeReverseMapOverlay(fn func(reverseKey []byte, value []byte)) {
	for k, v := range wb.rmapOverlay {
		fn([]byte(k), v)
	}
}

// Flush commits the batch and resets state.
func (wb *WriteBatch) Flush() error {
	if wb.batch == nil {
		return nil
	}

	err := wb.batch.Commit()
	wb.batch = nil
	wb.count = 0
	wb.eventZones = 0

	return err
}

// --- High-level write helpers ---

// WriteAccountTxMapping records that a transaction involves an account (any role).
func (wb *WriteBatch) WriteAccountTxMapping(kb *dal.KeyBuilder, ledgerName string, account string, txID uint64) error {
	key := AccountTxKey(kb, PrefixAccountTx, ledgerName, account, txID)

	return wb.put(key, nil)
}

// WriteSourceAccountTxMapping records that an account is a source in a transaction.
func (wb *WriteBatch) WriteSourceAccountTxMapping(kb *dal.KeyBuilder, ledgerName string, account string, txID uint64) error {
	key := AccountTxKey(kb, PrefixSourceAccountTx, ledgerName, account, txID)

	return wb.put(key, nil)
}

// WriteDestinationAccountTxMapping records that an account is a destination in a transaction.
func (wb *WriteBatch) WriteDestinationAccountTxMapping(kb *dal.KeyBuilder, ledgerName string, account string, txID uint64) error {
	key := AccountTxKey(kb, PrefixDestinationAccountTx, ledgerName, account, txID)

	return wb.put(key, nil)
}

// WriteAccountByAssetIndex records that an account has ever touched (assetBase,
// precision). The value is the writing fold's raft sequence — the account's
// FIRST touch, because the indexer dedups repeated cells before calling here
// (writeAccountByAssetDedup) and never rewrites an existing row. A pinned read
// admits the row only when that stamp is at or below its pin, which is what
// keeps an aligned snapshot that folded ahead of the main handle from serving
// members the handle cannot enrich (see compileAccountHasAssetCondition).
func (wb *WriteBatch) WriteAccountByAssetIndex(kb *dal.KeyBuilder, ledgerName, account, assetBase string, precision uint8) error {
	seq, err := wb.eventSequence()
	if err != nil {
		return err
	}

	key := AccountByAssetKey(kb, ledgerName, assetBase, precision, account)

	var stamp [8]byte
	binary.BigEndian.PutUint64(stamp[:], seq)

	return wb.put(key, stamp[:])
}

// InsertMetadataIndexV inserts a metadata index entry at an explicit
// forward-encoding version when the caller's lifecycle contract guarantees
// that (entity, metadataKey, version) has no prior entry. It deliberately
// emits no delete event and still writes the reverse-map overlay, so a later
// mutation in the same batch can resolve this insert through
// ReverseMapOverlay.
//
// Callers that cannot prove absence must use ReplaceMetadataIndexV with the
// authoritative old encoded value instead.
func (wb *WriteBatch) InsertMetadataIndexV(
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledgerName string, ns, metadataKey string,
	version uint32,
	encodedValue, entityID []byte,
) error {
	return wb.ReplaceMetadataIndexV(
		kb,
		reverseKey,
		ledgerName, ns, metadataKey,
		version,
		encodedValue, nil, entityID,
	)
}

// ReplaceMetadataIndexV replaces a metadata index entry at an explicit
// forward-encoding version. The reverseKey is supplied by the caller so
// dual-write call sites can target distinct rmap rows for v_current and
// v_pending. The old encoded value is the entry currently in the index
// (typically looked up via reverseMapValue on the indexer hot path; nil
// means "no prior entry to delete").
func (wb *WriteBatch) ReplaceMetadataIndexV(
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledgerName string, ns, metadataKey string,
	version uint32,
	newEncodedValue, oldEncodedValue, entityID []byte,
) error {
	// Same encoded value re-set: membership is identical at every pin and
	// the rmap row already holds newEncodedValue, so there is nothing to
	// write. Load-bearing for the schema rewrite racing a live dual-write
	// at the same sequence — a same-seq DEL cannot win against the
	// standing ADD (op ordering), so it must not be emitted.
	if bytes.Equal(oldEncodedValue, newEncodedValue) {
		return nil
	}

	nullFlagChanged := oldEncodedValue == nil ||
		isNullEncoded(oldEncodedValue) != isNullEncoded(newEncodedValue)

	if oldEncodedValue != nil {
		if err := wb.appendMetadataIndexEvent(kb, ledgerName, ns, metadataKey, version, oldEncodedValue, entityID, MetadataEventDel); err != nil {
			return err
		}

		if nullFlagChanged {
			if err := wb.appendEntityExistsEvent(kb, ledgerName, ns, metadataKey, version, isNullEncoded(oldEncodedValue), entityID, MetadataEventDel); err != nil {
				return err
			}
		}
	}

	if err := wb.appendMetadataIndexEvent(kb, ledgerName, ns, metadataKey, version, newEncodedValue, entityID, MetadataEventAdd); err != nil {
		return err
	}

	if nullFlagChanged {
		if err := wb.appendEntityExistsEvent(kb, ledgerName, ns, metadataKey, version, isNullEncoded(newEncodedValue), entityID, MetadataEventAdd); err != nil {
			return err
		}
	}

	if err := wb.put(reverseKey, newEncodedValue); err != nil {
		return err
	}

	wb.rmapOverlay[string(reverseKey)] = bytes.Clone(newEncodedValue)

	return nil
}

// DeleteMetadataEntryWithPreviousV removes both the forward index and the
// reverse-map entry for a metadata key on a specific entity at an explicit
// forward-encoding version.
func (wb *WriteBatch) DeleteMetadataEntryWithPreviousV(
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledgerName string, ns, metadataKey string,
	version uint32,
	oldEncodedValue, entityID []byte,
) error {
	if oldEncodedValue != nil {
		if err := wb.appendMetadataIndexEvent(kb, ledgerName, ns, metadataKey, version, oldEncodedValue, entityID, MetadataEventDel); err != nil {
			return err
		}

		if err := wb.appendEntityExistsEvent(kb, ledgerName, ns, metadataKey, version, isNullEncoded(oldEncodedValue), entityID, MetadataEventDel); err != nil {
			return err
		}
	}

	if err := wb.del(reverseKey); err != nil {
		return err
	}

	wb.rmapOverlay[string(reverseKey)] = nil

	return nil
}

// WriteTransactionReferenceIndex inserts an entry in the transaction reference index.
func (wb *WriteBatch) WriteTransactionReferenceIndex(kb *dal.KeyBuilder, ledgerName string, reference string, txID uint64) error {
	key := TransactionReferenceKey(kb, ledgerName, reference, txID)

	return wb.put(key, nil)
}

// WriteTransactionTimestampIndex inserts an entry in the transaction timestamp index.
func (wb *WriteBatch) WriteTransactionTimestampIndex(kb *dal.KeyBuilder, ledgerName string, timestamp, txID uint64) error {
	key := TransactionTimestampKey(kb, ledgerName, timestamp, txID)

	return wb.put(key, nil)
}

// WriteTransactionInsertedAtIndex inserts an entry in the transaction inserted_at index.
func (wb *WriteBatch) WriteTransactionInsertedAtIndex(kb *dal.KeyBuilder, ledgerName string, timestamp, txID uint64) error {
	key := TransactionInsertedAtKey(kb, ledgerName, timestamp, txID)

	return wb.put(key, nil)
}

// WriteTransactionRevertedAtIndex inserts an entry in the transaction
// reverted_at index. The value is the revert fold's raft sequence: unlike the
// other transaction builtins, this row appears AFTER the transaction's
// creation, so main-store existence proves nothing about it at a pin — the
// stamp is what lets a pinned read exclude a revert that folded past its
// handle (see compileRevertedAtCondition). A transaction reverts at most
// once, so the single write needs no dedup.
func (wb *WriteBatch) WriteTransactionRevertedAtIndex(kb *dal.KeyBuilder, ledgerName string, timestamp, txID uint64) error {
	seq, err := wb.eventSequence()
	if err != nil {
		return err
	}

	key := TransactionRevertedAtKey(kb, ledgerName, timestamp, txID)

	var stamp [8]byte
	binary.BigEndian.PutUint64(stamp[:], seq)

	return wb.put(key, stamp[:])
}

// WriteLedgerLogDateIndex inserts an entry in the per-ledger log date index.
func (wb *WriteBatch) WriteLedgerLogDateIndex(kb *dal.KeyBuilder, ledgerName string, timestamp, logID uint64) error {
	key := LedgerLogDateKey(kb, ledgerName, timestamp, logID)

	return wb.put(key, nil)
}

// WriteLedgerLogIndex inserts an entry in the per-ledger log index.
// The value is the global sequence, encoded as big-endian uint64.
func (wb *WriteBatch) WriteLedgerLogIndex(kb *dal.KeyBuilder, ledgerName string, logID, globalSequence uint64) error {
	key := LedgerLogKey(kb, ledgerName, logID)

	var val [8]byte
	binary.BigEndian.PutUint64(val[:], globalSequence)

	return wb.put(key, val[:])
}

// isNullEncoded returns true if the encoded value starts with TypeTagNull.
func isNullEncoded(encodedValue []byte) bool {
	return len(encodedValue) > 0 && encodedValue[0] == TypeTagNull
}
