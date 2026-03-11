package readstore

import (
	"github.com/cockroachdb/pebble"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// WriteMetadataIndexTo inserts a forward index entry in the metadata inverted index.
func WriteMetadataIndexTo(batch *pebble.Batch, kb *dal.KeyBuilder, ledger, ns, metadataKey string, encodedValue, entityID []byte) error {
	key := MetadataIndexKey(kb, ledger, ns, metadataKey, encodedValue, entityID)

	return batch.Set(key, nil, pebble.NoSync)
}

// DeleteMetadataIndexFrom removes a forward index entry from the metadata inverted index.
func DeleteMetadataIndexFrom(batch *pebble.Batch, kb *dal.KeyBuilder, ledger, ns, metadataKey string, encodedValue, entityID []byte) error {
	key := MetadataIndexKey(kb, ledger, ns, metadataKey, encodedValue, entityID)

	return batch.Delete(key, pebble.NoSync)
}

// ReadReverseMapFrom reads the current encoded value for an entity's metadata key
// from the reverse map in a Pebble reader. Returns nil if no entry exists.
func ReadReverseMapFrom(reader dal.PebbleReader, key []byte) []byte {
	v, closer, err := reader.Get(key)
	if err != nil {
		return nil
	}

	result := make([]byte, len(v))
	copy(result, v)
	_ = closer.Close()

	return result
}

// WriteReverseMapTo writes an encoded metadata value to the reverse map.
func WriteReverseMapTo(batch *pebble.Batch, key, encodedValue []byte) error {
	return batch.Set(key, encodedValue, pebble.NoSync)
}

// DeleteReverseMapFrom removes an entry from the reverse map.
func DeleteReverseMapFrom(batch *pebble.Batch, key []byte) error {
	return batch.Delete(key, pebble.NoSync)
}

// isNullEncoded returns true if the encoded value starts with TypeTagNull.
func isNullEncoded(encodedValue []byte) bool {
	return len(encodedValue) > 0 && encodedValue[0] == TypeTagNull
}

// WriteEntityExistsTo inserts an entry in the entity-ordered existence index.
func WriteEntityExistsTo(batch *pebble.Batch, kb *dal.KeyBuilder, ledger, ns, metaKey string, isNull bool, entityID []byte) error {
	key := EntityExistsKey(kb, ledger, ns, metaKey, isNull, entityID)

	return batch.Set(key, nil, pebble.NoSync)
}

// DeleteEntityExistsFrom removes an entry from the entity-ordered existence index.
func DeleteEntityExistsFrom(batch *pebble.Batch, kb *dal.KeyBuilder, ledger, ns, metaKey string, isNull bool, entityID []byte) error {
	key := EntityExistsKey(kb, ledger, ns, metaKey, isNull, entityID)

	return batch.Delete(key, pebble.NoSync)
}
