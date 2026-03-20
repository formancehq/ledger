package readstore

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// WriteBatch buffers Pebble write operations using an indexed batch.
// An indexed batch allows reads to see previously buffered writes,
// which is needed for the reverse map overlay pattern.
type WriteBatch struct {
	batch *pebble.Batch
	count int // number of operations buffered
}

// NewWriteBatch creates a new WriteBatch.
func NewWriteBatch() *WriteBatch {
	return &WriteBatch{}
}

// Init binds the batch to a Pebble indexed batch.
func (wb *WriteBatch) Init(batch *pebble.Batch) {
	wb.batch = batch
}

// Batch returns the underlying Pebble batch for direct operations (e.g., range deletes).
func (wb *WriteBatch) Batch() *pebble.Batch {
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
	if err := wb.batch.Set(key, value, pebble.NoSync); err != nil {
		return err
	}

	wb.count++

	return nil
}

// del deletes a key in the batch.
func (wb *WriteBatch) del(key []byte) error {
	if err := wb.batch.Delete(key, pebble.NoSync); err != nil {
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

	err := wb.batch.Commit(pebble.NoSync)
	wb.batch = nil
	wb.count = 0

	return err
}

// --- Reverse map reads via indexed batch ---

// readReverseMap reads a reverse map value from the indexed batch,
// which sees both committed data and uncommitted writes.
func (wb *WriteBatch) readReverseMap(key []byte) []byte {
	v, closer, err := wb.batch.Get(key)
	if err != nil {
		return nil
	}

	// Copy value since it's only valid until closer is closed.
	result := make([]byte, len(v))
	copy(result, v)
	_ = closer.Close() // closer.Close() on Pebble batch Get never returns an error

	return result
}

// putReverseMap sets a reverse map entry. The indexed batch makes it
// visible to subsequent readReverseMap calls in the same batch.
func (wb *WriteBatch) putReverseMap(key, value []byte) error {
	return wb.put(key, value)
}

// deleteReverseMap deletes a reverse map entry.
func (wb *WriteBatch) deleteReverseMap(key []byte) error {
	return wb.del(key)
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

// UpdateMetadataIndex performs the atomic 4-step metadata index update:
//  1. Read old value from reverse map (via indexed batch)
//  2. Delete old forward index + eidx entry (if exists)
//  3. Insert new forward index + eidx entry
//  4. Update reverse map with new value
func (wb *WriteBatch) UpdateMetadataIndex(
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledger, ns, metadataKey string,
	newEncodedValue, entityID []byte,
) error {
	// Step 1: Read old value from reverse map (batch-aware).
	oldEncodedValue := wb.readReverseMap(reverseKey)

	// Step 2: Delete old forward index + eidx entry (if exists).
	if oldEncodedValue != nil {
		if err := wb.DeleteMetadataIndex(kb, ledger, ns, metadataKey, oldEncodedValue, entityID); err != nil {
			return err
		}

		if err := wb.DeleteEntityExists(kb, ledger, ns, metadataKey, isNullEncoded(oldEncodedValue), entityID); err != nil {
			return err
		}
	}

	// Step 3: Insert new forward index + eidx entry.
	if err := wb.WriteMetadataIndex(kb, ledger, ns, metadataKey, newEncodedValue, entityID); err != nil {
		return err
	}

	if err := wb.WriteEntityExists(kb, ledger, ns, metadataKey, isNullEncoded(newEncodedValue), entityID); err != nil {
		return err
	}

	// Step 4: Update reverse map.
	return wb.putReverseMap(reverseKey, newEncodedValue)
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

	return wb.putReverseMap(reverseKey, newEncodedValue)
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

	return wb.deleteReverseMap(reverseKey)
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

// DeleteMetadataEntry removes both the forward index and reverse map entries
// for a metadata key on a specific entity.
func (wb *WriteBatch) DeleteMetadataEntry(
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledger, ns, metadataKey string,
	entityID []byte,
) error {
	// Read old value from reverse map (batch-aware).
	oldEncodedValue := wb.readReverseMap(reverseKey)

	// Delete forward index + eidx entry (if exists).
	if oldEncodedValue != nil {
		if err := wb.DeleteMetadataIndex(kb, ledger, ns, metadataKey, oldEncodedValue, entityID); err != nil {
			return err
		}

		if err := wb.DeleteEntityExists(kb, ledger, ns, metadataKey, isNullEncoded(oldEncodedValue), entityID); err != nil {
			return err
		}
	}

	// Delete reverse map entry.
	return wb.deleteReverseMap(reverseKey)
}
