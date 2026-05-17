package readstore

import (
	"encoding/binary"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// WriteBatch buffers Pebble write operations using a dal.Batch.
type WriteBatch struct {
	batch *dal.Batch
	count int // number of operations buffered
}

// NewWriteBatch creates a new WriteBatch.
func NewWriteBatch() *WriteBatch {
	return &WriteBatch{}
}

// Init binds the batch to a dal.Batch.
func (wb *WriteBatch) Init(batch *dal.Batch) {
	wb.batch = batch
}

// Batch returns the underlying dal.Batch for direct operations (e.g., range deletes).
func (wb *WriteBatch) Batch() *dal.Batch {
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
func (wb *WriteBatch) WriteAccountTxMapping(kb *dal.KeyBuilder, ledger, account string, txID uint64) error {
	key := AccountTxKey(kb, PrefixAccountTx, ledger, account, txID)

	return wb.put(key, nil)
}

// WriteSourceAccountTxMapping records that an account is a source in a transaction.
func (wb *WriteBatch) WriteSourceAccountTxMapping(kb *dal.KeyBuilder, ledger, account string, txID uint64) error {
	key := AccountTxKey(kb, PrefixSourceAccountTx, ledger, account, txID)

	return wb.put(key, nil)
}

// WriteDestAccountTxMapping records that an account is a destination in a transaction.
func (wb *WriteBatch) WriteDestAccountTxMapping(kb *dal.KeyBuilder, ledger, account string, txID uint64) error {
	key := AccountTxKey(kb, PrefixDestAccountTx, ledger, account, txID)

	return wb.put(key, nil)
}

// WriteMetadataIndex inserts a forward index entry in the metadata inverted index.
func (wb *WriteBatch) WriteMetadataIndex(kb *dal.KeyBuilder, ledger, ns, metadataKey string, encodedValue, entityID []byte) error {
	key := MetadataIndexKey(kb, ledger, ns, metadataKey, encodedValue, entityID)

	return wb.put(key, nil)
}

// DeleteMetadataIndex removes a forward index entry from the metadata inverted index.
func (wb *WriteBatch) DeleteMetadataIndex(kb *dal.KeyBuilder, ledger, ns, metadataKey string, encodedValue, entityID []byte) error {
	key := MetadataIndexKey(kb, ledger, ns, metadataKey, encodedValue, entityID)

	return wb.del(key)
}

// WriteEntityExists inserts an entry in the entity-ordered existence index.
func (wb *WriteBatch) WriteEntityExists(kb *dal.KeyBuilder, ledger, ns, metaKey string, isNull bool, entityID []byte) error {
	key := EntityExistsKey(kb, ledger, ns, metaKey, isNull, entityID)

	return wb.put(key, nil)
}

// DeleteEntityExists removes an entry from the entity-ordered existence index.
func (wb *WriteBatch) DeleteEntityExists(kb *dal.KeyBuilder, ledger, ns, metaKey string, isNull bool, entityID []byte) error {
	key := EntityExistsKey(kb, ledger, ns, metaKey, isNull, entityID)

	return wb.del(key)
}

// ReplaceMetadataIndex replaces a metadata index entry using an explicit old value
// from the log, avoiding a reverse map read.
func (wb *WriteBatch) ReplaceMetadataIndex(
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledger, ns, metadataKey string,
	newEncodedValue, oldEncodedValue, entityID []byte,
) error {
	if oldEncodedValue != nil {
		if err := wb.DeleteMetadataIndex(kb, ledger, ns, metadataKey, oldEncodedValue, entityID); err != nil {
			return err
		}

		if err := wb.DeleteEntityExists(kb, ledger, ns, metadataKey, isNullEncoded(oldEncodedValue), entityID); err != nil {
			return err
		}
	}

	if err := wb.WriteMetadataIndex(kb, ledger, ns, metadataKey, newEncodedValue, entityID); err != nil {
		return err
	}

	if err := wb.WriteEntityExists(kb, ledger, ns, metadataKey, isNullEncoded(newEncodedValue), entityID); err != nil {
		return err
	}

	return wb.put(reverseKey, newEncodedValue)
}

// DeleteMetadataEntryWithPrevious removes both the forward index and reverse map entries
// for a metadata key on a specific entity, using an explicit old value from the log.
func (wb *WriteBatch) DeleteMetadataEntryWithPrevious(
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledger, ns, metadataKey string,
	oldEncodedValue, entityID []byte,
) error {
	if oldEncodedValue != nil {
		if err := wb.DeleteMetadataIndex(kb, ledger, ns, metadataKey, oldEncodedValue, entityID); err != nil {
			return err
		}

		if err := wb.DeleteEntityExists(kb, ledger, ns, metadataKey, isNullEncoded(oldEncodedValue), entityID); err != nil {
			return err
		}
	}

	return wb.del(reverseKey)
}

// WriteTransactionReferenceIndex inserts an entry in the transaction reference index.
func (wb *WriteBatch) WriteTransactionReferenceIndex(kb *dal.KeyBuilder, ledger, reference string, txID uint64) error {
	key := TransactionReferenceKey(kb, ledger, reference, txID)

	return wb.put(key, nil)
}

// WriteTransactionTimestampIndex inserts an entry in the transaction timestamp index.
func (wb *WriteBatch) WriteTransactionTimestampIndex(kb *dal.KeyBuilder, ledger string, timestamp, txID uint64) error {
	key := TransactionTimestampKey(kb, ledger, timestamp, txID)

	return wb.put(key, nil)
}

// WriteTransactionInsertedAtIndex inserts an entry in the transaction inserted_at index.
func (wb *WriteBatch) WriteTransactionInsertedAtIndex(kb *dal.KeyBuilder, ledger string, timestamp, txID uint64) error {
	key := TransactionInsertedAtKey(kb, ledger, timestamp, txID)

	return wb.put(key, nil)
}

// WriteLedgerLogDateIndex inserts an entry in the per-ledger log date index.
func (wb *WriteBatch) WriteLedgerLogDateIndex(kb *dal.KeyBuilder, ledger string, timestamp, logID uint64) error {
	key := LedgerLogDateKey(kb, ledger, timestamp, logID)

	return wb.put(key, nil)
}

// WriteLedgerLogIndex inserts an entry in the per-ledger log index.
// The value is the global sequence, encoded as big-endian uint64.
func (wb *WriteBatch) WriteLedgerLogIndex(kb *dal.KeyBuilder, ledger string, logID, globalSequence uint64) error {
	key := LedgerLogKey(kb, ledger, logID)

	var val [8]byte
	binary.BigEndian.PutUint64(val[:], globalSequence)

	return wb.put(key, val[:])
}

// isNullEncoded returns true if the encoded value starts with TypeTagNull.
func isNullEncoded(encodedValue []byte) bool {
	return len(encodedValue) > 0 && encodedValue[0] == TypeTagNull
}
