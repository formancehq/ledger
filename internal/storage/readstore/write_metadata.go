package readstore

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// WriteMetadataIndex inserts a forward index entry in the metadata inverted index.
//
//	Key: [ledger\x00][ns:][metadataKey\x00][typeTag][sortableValue][entityID]
//	Value: (empty)
func WriteMetadataIndex(tx *bolt.Tx, kb *dal.KeyBuilder, ledger, ns, metadataKey string, encodedValue, entityID []byte) error {
	b := tx.Bucket(BucketMetadataIndex)
	key := MetadataIndexKey(kb, ledger, ns, metadataKey, encodedValue, entityID)

	return b.Put(key, nil)
}

// DeleteMetadataIndex removes a forward index entry from the metadata inverted index.
func DeleteMetadataIndex(tx *bolt.Tx, kb *dal.KeyBuilder, ledger, ns, metadataKey string, encodedValue, entityID []byte) error {
	b := tx.Bucket(BucketMetadataIndex)
	key := MetadataIndexKey(kb, ledger, ns, metadataKey, encodedValue, entityID)

	return b.Delete(key)
}

// ReadReverseMap reads the current encoded value for an entity's metadata key
// from the reverse map. Returns nil if no entry exists.
func ReadReverseMap(tx *bolt.Tx, key []byte) []byte {
	b := tx.Bucket(BucketReverseMap)

	return b.Get(key)
}

// WriteReverseMap writes an encoded metadata value to the reverse map.
func WriteReverseMap(tx *bolt.Tx, key, encodedValue []byte) error {
	b := tx.Bucket(BucketReverseMap)

	return b.Put(key, encodedValue)
}

// DeleteReverseMap removes an entry from the reverse map.
func DeleteReverseMap(tx *bolt.Tx, key []byte) error {
	b := tx.Bucket(BucketReverseMap)

	return b.Delete(key)
}

// isNullEncoded returns true if the encoded value starts with TypeTagNull.
func isNullEncoded(encodedValue []byte) bool {
	return len(encodedValue) > 0 && encodedValue[0] == TypeTagNull
}

// WriteEntityExists inserts an entry in the entity-ordered existence index.
func WriteEntityExists(tx *bolt.Tx, kb *dal.KeyBuilder, ledger, ns, metaKey string, isNull bool, entityID []byte) error {
	b := tx.Bucket(BucketEntityExists)
	key := EntityExistsKey(kb, ledger, ns, metaKey, isNull, entityID)

	return b.Put(key, nil)
}

// DeleteEntityExists removes an entry from the entity-ordered existence index.
func DeleteEntityExists(tx *bolt.Tx, kb *dal.KeyBuilder, ledger, ns, metaKey string, isNull bool, entityID []byte) error {
	b := tx.Bucket(BucketEntityExists)
	key := EntityExistsKey(kb, ledger, ns, metaKey, isNull, entityID)

	return b.Delete(key)
}

// UpdateMetadataIndex performs the atomic 4-step metadata index update:
//  1. Read old value from reverse map
//  2. Delete old forward index entry (if exists)
//  3. Insert new forward index entry
//  4. Update reverse map with new value
//
// Parameters:
//   - reverseKey: pre-built reverse map key
//   - ledger, ns, metadataKey: for building forward index keys
//   - newEncodedValue: type-tagged sortable encoding of the new value
//   - entityID: account address bytes or txID bytes
func UpdateMetadataIndex(
	tx *bolt.Tx,
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledger, ns, metadataKey string,
	newEncodedValue, entityID []byte,
) error {
	// Step 1: Read old value from reverse map
	oldEncodedValue := ReadReverseMap(tx, reverseKey)

	// Step 2: Delete old forward index + eidx entry (if exists)
	if oldEncodedValue != nil {
		err := DeleteMetadataIndex(tx, kb, ledger, ns, metadataKey, oldEncodedValue, entityID)
		if err != nil {
			return fmt.Errorf("deleting old metadata index: %w", err)
		}

		err = DeleteEntityExists(tx, kb, ledger, ns, metadataKey, isNullEncoded(oldEncodedValue), entityID)
		if err != nil {
			return fmt.Errorf("deleting old entity exists index: %w", err)
		}
	}

	// Step 3: Insert new forward index + eidx entry
	err := WriteMetadataIndex(tx, kb, ledger, ns, metadataKey, newEncodedValue, entityID)
	if err != nil {
		return fmt.Errorf("writing new metadata index: %w", err)
	}

	err = WriteEntityExists(tx, kb, ledger, ns, metadataKey, isNullEncoded(newEncodedValue), entityID)
	if err != nil {
		return fmt.Errorf("writing new entity exists index: %w", err)
	}

	// Step 4: Update reverse map
	err = WriteReverseMap(tx, reverseKey, newEncodedValue)
	if err != nil {
		return fmt.Errorf("updating reverse map: %w", err)
	}

	return nil
}

// DeleteMetadataEntry removes both the forward index and reverse map entries
// for a metadata key on a specific entity.
func DeleteMetadataEntry(
	tx *bolt.Tx,
	kb *dal.KeyBuilder,
	reverseKey []byte,
	ledger, ns, metadataKey string,
	entityID []byte,
) error {
	// Read old value from reverse map
	oldEncodedValue := ReadReverseMap(tx, reverseKey)

	// Delete forward index + eidx entry (if exists)
	if oldEncodedValue != nil {
		err := DeleteMetadataIndex(tx, kb, ledger, ns, metadataKey, oldEncodedValue, entityID)
		if err != nil {
			return fmt.Errorf("deleting metadata index: %w", err)
		}

		err = DeleteEntityExists(tx, kb, ledger, ns, metadataKey, isNullEncoded(oldEncodedValue), entityID)
		if err != nil {
			return fmt.Errorf("deleting entity exists index: %w", err)
		}
	}

	// Delete reverse map entry
	err := DeleteReverseMap(tx, reverseKey)
	if err != nil {
		return fmt.Errorf("deleting reverse map: %w", err)
	}

	return nil
}
