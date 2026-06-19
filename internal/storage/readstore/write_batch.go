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

// WriteMetadataIndex inserts a forward index entry in the metadata inverted index.
func (wb *WriteBatch) WriteMetadataIndex(kb *dal.KeyBuilder, ledgerName string, ns, metadataKey string, encodedValue, entityID []byte) error {
	key := MetadataIndexKey(kb, ledgerName, ns, metadataKey, encodedValue, entityID)

	return wb.put(key, nil)
}

// DeleteMetadataIndex removes a forward index entry from the metadata inverted index.
func (wb *WriteBatch) DeleteMetadataIndex(kb *dal.KeyBuilder, ledgerName string, ns, metadataKey string, encodedValue, entityID []byte) error {
	key := MetadataIndexKey(kb, ledgerName, ns, metadataKey, encodedValue, entityID)

	return wb.del(key)
}

// WriteEntityExists inserts an entry in the entity-ordered existence index.
func (wb *WriteBatch) WriteEntityExists(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string, isNull bool, entityID []byte) error {
	key := EntityExistsKey(kb, ledgerName, ns, metaKey, isNull, entityID)

	return wb.put(key, nil)
}

// DeleteEntityExists removes an entry from the entity-ordered existence index.
func (wb *WriteBatch) DeleteEntityExists(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string, isNull bool, entityID []byte) error {
	key := EntityExistsKey(kb, ledgerName, ns, metaKey, isNull, entityID)

	return wb.del(key)
}

// ReplaceMetadataIndex replaces a metadata index entry using an explicit old value
// from the log, avoiding a reverse map read.
func (wb *WriteBatch) ReplaceMetadataIndex(
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledgerName string, ns, metadataKey string,
	newEncodedValue, oldEncodedValue, entityID []byte,
) error {
	if oldEncodedValue != nil {
		if err := wb.DeleteMetadataIndex(kb, ledgerName, ns, metadataKey, oldEncodedValue, entityID); err != nil {
			return err
		}

		if err := wb.DeleteEntityExists(kb, ledgerName, ns, metadataKey, isNullEncoded(oldEncodedValue), entityID); err != nil {
			return err
		}
	}

	if err := wb.WriteMetadataIndex(kb, ledgerName, ns, metadataKey, newEncodedValue, entityID); err != nil {
		return err
	}

	if err := wb.WriteEntityExists(kb, ledgerName, ns, metadataKey, isNullEncoded(newEncodedValue), entityID); err != nil {
		return err
	}

	if err := wb.put(reverseKey, newEncodedValue); err != nil {
		return err
	}

	wb.rmapOverlay[string(reverseKey)] = bytes.Clone(newEncodedValue)

	return nil
}

// DeleteMetadataEntryWithPrevious removes both the forward index and reverse map entries
// for a metadata key on a specific entity, using an explicit old value from the log.
func (wb *WriteBatch) DeleteMetadataEntryWithPrevious(
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledgerName string, ns, metadataKey string,
	oldEncodedValue, entityID []byte,
) error {
	if oldEncodedValue != nil {
		if err := wb.DeleteMetadataIndex(kb, ledgerName, ns, metadataKey, oldEncodedValue, entityID); err != nil {
			return err
		}

		if err := wb.DeleteEntityExists(kb, ledgerName, ns, metadataKey, isNullEncoded(oldEncodedValue), entityID); err != nil {
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
