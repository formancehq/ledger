package readstore

import (
	"bytes"
	"encoding/binary"
	"sort"

	bolt "go.etcd.io/bbolt"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// Bucket indexes for WriteBatch. Each index corresponds to one bbolt bucket.
const (
	batchBucketMidx  = iota // BucketMetadataIndex
	batchBucketEidx         // BucketEntityExists
	batchBucketRmap         // BucketReverseMap
	batchBucketAtxm         // BucketAccountTx
	batchBucketSatx         // BucketSourceAccountTx
	batchBucketDatx         // BucketDestAccountTx
	batchBucketTxref        // BucketTransactionReference
	batchBucketTstmp        // BucketTransactionTimestamp
	batchBucketLlog         // BucketLedgerLogs
	batchBucketLldt         // BucketLedgerLogDate
	numBatchBuckets
)

// batchBucketNames maps bucket indexes to their bbolt bucket names.
var batchBucketNames = [numBatchBuckets][]byte{
	BucketMetadataIndex,
	BucketEntityExists,
	BucketReverseMap,
	BucketAccountTx,
	BucketSourceAccountTx,
	BucketDestAccountTx,
	BucketTransactionReference,
	BucketTransactionTimestamp,
	BucketLedgerLogs,
	BucketLedgerLogDate,
}

// writeOp represents a buffered write or delete operation.
type writeOp struct {
	key    []byte
	value  []byte // nil = empty value (existence entries); non-nil = actual value
	delete bool
}

// WriteBatch buffers bbolt write operations and flushes them sorted by key
// to minimize random B+ tree page access. This dramatically improves write
// throughput when keys have high entropy (e.g. UUID-based account addresses).
//
// For the reverse map bucket, an in-memory overlay ensures that reads within
// the same batch see previously buffered writes.
type WriteBatch struct {
	tx *bolt.Tx

	// Per-bucket operation buffers. Key is string(key) for last-writer-wins dedup.
	ops [numBatchBuckets]map[string]writeOp

	// In-memory overlay for reverse map reads within the batch.
	// A nil value means the entry was deleted in this batch.
	// Absence from the map means "not written in this batch — read from bbolt".
	rmapOverlay map[string][]byte
	// rmapDeleted tracks keys explicitly deleted (since nil value in rmapOverlay
	// is ambiguous with "not present"). We use a separate set.
	rmapDeleted map[string]struct{}
}

// NewWriteBatch creates a new WriteBatch. Reuse across transactions by calling
// Reset() between batches.
func NewWriteBatch() *WriteBatch {
	wb := &WriteBatch{
		rmapOverlay: make(map[string][]byte, 256),
		rmapDeleted: make(map[string]struct{}, 16),
	}
	for i := range wb.ops {
		wb.ops[i] = make(map[string]writeOp, 256)
	}

	return wb
}

// Init binds the batch to a bbolt transaction. Must be called at the start of
// each db.Update() callback.
func (wb *WriteBatch) Init(tx *bolt.Tx) {
	wb.tx = tx
}

// Empty returns true if no operations have been buffered.
func (wb *WriteBatch) Empty() bool {
	for i := range wb.ops {
		if len(wb.ops[i]) > 0 {
			return false
		}
	}

	return true
}

// Reset clears all buffered operations and the overlay, keeping allocated maps.
func (wb *WriteBatch) Reset() {
	for i := range wb.ops {
		clear(wb.ops[i])
	}

	clear(wb.rmapOverlay)
	clear(wb.rmapDeleted)
	wb.tx = nil
}

// put buffers a Put operation for the given bucket.
func (wb *WriteBatch) put(bucketIdx int, key, value []byte) {
	wb.ops[bucketIdx][string(key)] = writeOp{key: key, value: value}
}

// del buffers a Delete operation for the given bucket.
func (wb *WriteBatch) del(bucketIdx int, key []byte) {
	wb.ops[bucketIdx][string(key)] = writeOp{key: key, delete: true}
}

// Flush sorts all buffered operations per bucket by key, then executes them
// against bbolt. Operations on the same key are deduplicated (last writer wins).
// After flushing, the batch is reset.
func (wb *WriteBatch) Flush() error {
	for i := range numBatchBuckets {
		ops := wb.ops[i]
		if len(ops) == 0 {
			continue
		}

		bucket := wb.tx.Bucket(batchBucketNames[i])

		// Collect into a sortable slice.
		sorted := make([]writeOp, 0, len(ops))
		for _, op := range ops {
			sorted = append(sorted, op)
		}

		sort.Slice(sorted, func(a, b int) bool {
			return bytes.Compare(sorted[a].key, sorted[b].key) < 0
		})

		// Execute in key order for maximum B+ tree locality.
		for _, op := range sorted {
			if op.delete {
				err := bucket.Delete(op.key)
				if err != nil {
					return err
				}
			} else {
				err := bucket.Put(op.key, op.value)
				if err != nil {
					return err
				}
			}
		}
	}

	wb.Reset()

	return nil
}

// --- Reverse map overlay ---

// readReverseMap reads a reverse map value, checking the in-memory overlay first.
func (wb *WriteBatch) readReverseMap(key []byte) []byte {
	k := string(key)
	// Check if deleted in this batch.
	if _, deleted := wb.rmapDeleted[k]; deleted {
		return nil
	}
	// Check overlay.
	if v, ok := wb.rmapOverlay[k]; ok {
		return v
	}
	// Fall back to bbolt.
	return wb.tx.Bucket(BucketReverseMap).Get(key)
}

// putReverseMap buffers a reverse map write and updates the overlay.
func (wb *WriteBatch) putReverseMap(key, value []byte) {
	k := string(key)
	wb.rmapOverlay[k] = value
	delete(wb.rmapDeleted, k)
	wb.put(batchBucketRmap, key, value)
}

// deleteReverseMap buffers a reverse map delete and updates the overlay.
func (wb *WriteBatch) deleteReverseMap(key []byte) {
	k := string(key)
	wb.rmapDeleted[k] = struct{}{}
	delete(wb.rmapOverlay, k)
	wb.del(batchBucketRmap, key)
}

// --- High-level write helpers (mirror the package-level Write* functions) ---

// WriteAccountTxMapping records that a transaction involves an account (any role).
func (wb *WriteBatch) WriteAccountTxMapping(kb *dal.KeyBuilder, ledger, account string, txID uint64) {
	key := AccountTxKey(kb, ledger, account, txID)
	wb.put(batchBucketAtxm, key, nil)
}

// WriteSourceAccountTxMapping records that an account is a source in a transaction.
func (wb *WriteBatch) WriteSourceAccountTxMapping(kb *dal.KeyBuilder, ledger, account string, txID uint64) {
	key := AccountTxKey(kb, ledger, account, txID)
	wb.put(batchBucketSatx, key, nil)
}

// WriteDestAccountTxMapping records that an account is a destination in a transaction.
func (wb *WriteBatch) WriteDestAccountTxMapping(kb *dal.KeyBuilder, ledger, account string, txID uint64) {
	key := AccountTxKey(kb, ledger, account, txID)
	wb.put(batchBucketDatx, key, nil)
}

// WriteMetadataIndex inserts a forward index entry in the metadata inverted index.
func (wb *WriteBatch) WriteMetadataIndex(kb *dal.KeyBuilder, ledger, ns, metadataKey string, encodedValue, entityID []byte) {
	key := MetadataIndexKey(kb, ledger, ns, metadataKey, encodedValue, entityID)
	wb.put(batchBucketMidx, key, nil)
}

// DeleteMetadataIndex removes a forward index entry from the metadata inverted index.
func (wb *WriteBatch) DeleteMetadataIndex(kb *dal.KeyBuilder, ledger, ns, metadataKey string, encodedValue, entityID []byte) {
	key := MetadataIndexKey(kb, ledger, ns, metadataKey, encodedValue, entityID)
	wb.del(batchBucketMidx, key)
}

// WriteEntityExists inserts an entry in the entity-ordered existence index.
func (wb *WriteBatch) WriteEntityExists(kb *dal.KeyBuilder, ledger, ns, metaKey string, isNull bool, entityID []byte) {
	key := EntityExistsKey(kb, ledger, ns, metaKey, isNull, entityID)
	wb.put(batchBucketEidx, key, nil)
}

// DeleteEntityExists removes an entry from the entity-ordered existence index.
func (wb *WriteBatch) DeleteEntityExists(kb *dal.KeyBuilder, ledger, ns, metaKey string, isNull bool, entityID []byte) {
	key := EntityExistsKey(kb, ledger, ns, metaKey, isNull, entityID)
	wb.del(batchBucketEidx, key)
}

// UpdateMetadataIndex performs the atomic 4-step metadata index update:
//  1. Read old value from reverse map (with overlay)
//  2. Delete old forward index + eidx entry (if exists)
//  3. Insert new forward index + eidx entry
//  4. Update reverse map with new value
func (wb *WriteBatch) UpdateMetadataIndex(
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledger, ns, metadataKey string,
	newEncodedValue, entityID []byte,
) {
	// Step 1: Read old value from reverse map (overlay-aware).
	oldEncodedValue := wb.readReverseMap(reverseKey)

	// Step 2: Delete old forward index + eidx entry (if exists).
	if oldEncodedValue != nil {
		wb.DeleteMetadataIndex(kb, ledger, ns, metadataKey, oldEncodedValue, entityID)
		wb.DeleteEntityExists(kb, ledger, ns, metadataKey, isNullEncoded(oldEncodedValue), entityID)
	}

	// Step 3: Insert new forward index + eidx entry.
	wb.WriteMetadataIndex(kb, ledger, ns, metadataKey, newEncodedValue, entityID)
	wb.WriteEntityExists(kb, ledger, ns, metadataKey, isNullEncoded(newEncodedValue), entityID)

	// Step 4: Update reverse map.
	wb.putReverseMap(reverseKey, newEncodedValue)
}

// WriteTransactionReferenceIndex inserts an entry in the transaction reference index.
func (wb *WriteBatch) WriteTransactionReferenceIndex(kb *dal.KeyBuilder, ledger, reference string, txID uint64) {
	key := TransactionReferenceKey(kb, ledger, reference, txID)
	wb.put(batchBucketTxref, key, nil)
}

// WriteTransactionTimestampIndex inserts an entry in the transaction timestamp index.
func (wb *WriteBatch) WriteTransactionTimestampIndex(kb *dal.KeyBuilder, ledger string, timestamp, txID uint64) {
	key := TransactionTimestampKey(kb, ledger, timestamp, txID)
	wb.put(batchBucketTstmp, key, nil)
}

// WriteLedgerLogDateIndex inserts an entry in the per-ledger log date index.
func (wb *WriteBatch) WriteLedgerLogDateIndex(kb *dal.KeyBuilder, ledger string, timestamp, logID uint64) {
	key := LedgerLogDateKey(kb, ledger, timestamp, logID)
	wb.put(batchBucketLldt, key, nil)
}

// WriteLedgerLogIndex inserts an entry in the per-ledger log index.
// The value is the global sequence, encoded as big-endian uint64.
func (wb *WriteBatch) WriteLedgerLogIndex(kb *dal.KeyBuilder, ledger string, logID, globalSequence uint64) {
	key := LedgerLogKey(kb, ledger, logID)

	var val [8]byte
	binary.BigEndian.PutUint64(val[:], globalSequence)
	wb.put(batchBucketLlog, key, val[:])
}

// DeleteMetadataEntry removes both the forward index and reverse map entries
// for a metadata key on a specific entity.
func (wb *WriteBatch) DeleteMetadataEntry(
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledger, ns, metadataKey string,
	entityID []byte,
) {
	// Read old value from reverse map (overlay-aware).
	oldEncodedValue := wb.readReverseMap(reverseKey)

	// Delete forward index + eidx entry (if exists).
	if oldEncodedValue != nil {
		wb.DeleteMetadataIndex(kb, ledger, ns, metadataKey, oldEncodedValue, entityID)
		wb.DeleteEntityExists(kb, ledger, ns, metadataKey, isNullEncoded(oldEncodedValue), entityID)
	}

	// Delete reverse map entry.
	wb.deleteReverseMap(reverseKey)
}
