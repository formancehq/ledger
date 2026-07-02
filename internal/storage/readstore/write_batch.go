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

	// rmapOverlay is a read-your-writes view of the reverse-map mutations made in
	// the current (uncommitted) batch: reverseKey -> encoded value last written
	// (nil = deleted). It is reset by Init so it always matches the bound batch,
	// and is written by ReplaceMetadataIndex/DeleteMetadataEntryWithPrevious
	// themselves — so a reverse-map write that is not mirrored is unrepresentable.
	// Callers resolve the index's current value for a key via ReverseMapOverlay
	// (uncommitted batch) before falling back to committed state.
	rmapOverlay map[string][]byte
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

// WriteDestAccountTxMapping records that an account is a destination in a transaction.
func (wb *WriteBatch) WriteDestAccountTxMapping(kb *dal.KeyBuilder, ledgerName string, account string, txID uint64) error {
	key := AccountTxKey(kb, PrefixDestAccountTx, ledgerName, account, txID)

	return wb.put(key, nil)
}

// WriteAccountByAssetIndex records that an account has ever touched (assetBase,
// precision). Presence-only (nil value); the Put is idempotent so repeated
// writes for the same cell are harmless.
func (wb *WriteBatch) WriteAccountByAssetIndex(kb *dal.KeyBuilder, ledgerName, account, assetBase string, precision uint8) error {
	key := AccountByAssetKey(kb, ledgerName, assetBase, precision, account)

	return wb.put(key, nil)
}

// WriteMetadataIndex inserts a forward index entry in the metadata inverted
// index under version 1. Equivalent to WriteMetadataIndexV(..., 1, ...) —
// kept for callers not yet aware of versioning.
func (wb *WriteBatch) WriteMetadataIndex(kb *dal.KeyBuilder, ledgerName string, ns, metadataKey string, encodedValue, entityID []byte) error {
	return wb.WriteMetadataIndexV(kb, ledgerName, ns, metadataKey, 1, encodedValue, entityID)
}

// WriteMetadataIndexV inserts a forward index entry at an explicit
// forward-encoding version. Used by the indexer hot path to write
// against local_current_version and (during a rewrite) pending_version.
func (wb *WriteBatch) WriteMetadataIndexV(kb *dal.KeyBuilder, ledgerName string, ns, metadataKey string, version uint32, encodedValue, entityID []byte) error {
	key := MetadataIndexKeyV(kb, ledgerName, ns, metadataKey, version, encodedValue, entityID)

	return wb.put(key, nil)
}

// DeleteMetadataIndex removes a forward index entry under version 1.
func (wb *WriteBatch) DeleteMetadataIndex(kb *dal.KeyBuilder, ledgerName string, ns, metadataKey string, encodedValue, entityID []byte) error {
	return wb.DeleteMetadataIndexV(kb, ledgerName, ns, metadataKey, 1, encodedValue, entityID)
}

// DeleteMetadataIndexV is the version-aware variant of DeleteMetadataIndex.
func (wb *WriteBatch) DeleteMetadataIndexV(kb *dal.KeyBuilder, ledgerName string, ns, metadataKey string, version uint32, encodedValue, entityID []byte) error {
	key := MetadataIndexKeyV(kb, ledgerName, ns, metadataKey, version, encodedValue, entityID)

	return wb.del(key)
}

// WriteEntityExists inserts an entry in the entity-ordered existence index
// under version 1.
func (wb *WriteBatch) WriteEntityExists(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string, isNull bool, entityID []byte) error {
	return wb.WriteEntityExistsV(kb, ledgerName, ns, metaKey, 1, isNull, entityID)
}

// WriteEntityExistsV is the version-aware variant of WriteEntityExists.
func (wb *WriteBatch) WriteEntityExistsV(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string, version uint32, isNull bool, entityID []byte) error {
	key := EntityExistsKeyV(kb, ledgerName, ns, metaKey, version, isNull, entityID)

	return wb.put(key, nil)
}

// DeleteEntityExists removes an entry from the entity-ordered existence index
// under version 1.
func (wb *WriteBatch) DeleteEntityExists(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string, isNull bool, entityID []byte) error {
	return wb.DeleteEntityExistsV(kb, ledgerName, ns, metaKey, 1, isNull, entityID)
}

// DeleteEntityExistsV is the version-aware variant of DeleteEntityExists.
func (wb *WriteBatch) DeleteEntityExistsV(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string, version uint32, isNull bool, entityID []byte) error {
	key := EntityExistsKeyV(kb, ledgerName, ns, metaKey, version, isNull, entityID)

	return wb.del(key)
}

// ReplaceMetadataIndex replaces a metadata index entry at version 1 (the
// default for callers that have not been versioned yet). See
// ReplaceMetadataIndexV for the explicit-version variant used by the
// indexer hot path.
func (wb *WriteBatch) ReplaceMetadataIndex(
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledgerName string, ns, metadataKey string,
	newEncodedValue, oldEncodedValue, entityID []byte,
) error {
	return wb.ReplaceMetadataIndexV(kb, reverseKey, ledgerName, ns, metadataKey, 1, newEncodedValue, oldEncodedValue, entityID)
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
	if oldEncodedValue != nil {
		if err := wb.DeleteMetadataIndexV(kb, ledgerName, ns, metadataKey, version, oldEncodedValue, entityID); err != nil {
			return err
		}

		if err := wb.DeleteEntityExistsV(kb, ledgerName, ns, metadataKey, version, isNullEncoded(oldEncodedValue), entityID); err != nil {
			return err
		}
	}

	if err := wb.WriteMetadataIndexV(kb, ledgerName, ns, metadataKey, version, newEncodedValue, entityID); err != nil {
		return err
	}

	if err := wb.WriteEntityExistsV(kb, ledgerName, ns, metadataKey, version, isNullEncoded(newEncodedValue), entityID); err != nil {
		return err
	}

	if err := wb.put(reverseKey, newEncodedValue); err != nil {
		return err
	}

	wb.rmapOverlay[string(reverseKey)] = bytes.Clone(newEncodedValue)

	return nil
}

// DeleteMetadataEntryWithPrevious removes a metadata entry at version 1.
// See DeleteMetadataEntryWithPreviousV for the explicit-version variant.
func (wb *WriteBatch) DeleteMetadataEntryWithPrevious(
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledgerName string, ns, metadataKey string,
	oldEncodedValue, entityID []byte,
) error {
	return wb.DeleteMetadataEntryWithPreviousV(kb, reverseKey, ledgerName, ns, metadataKey, 1, oldEncodedValue, entityID)
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
		if err := wb.DeleteMetadataIndexV(kb, ledgerName, ns, metadataKey, version, oldEncodedValue, entityID); err != nil {
			return err
		}

		if err := wb.DeleteEntityExistsV(kb, ledgerName, ns, metadataKey, version, isNullEncoded(oldEncodedValue), entityID); err != nil {
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
