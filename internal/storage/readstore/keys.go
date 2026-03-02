package readstore

import "encoding/binary"

// Bucket names for bbolt. Each bucket is a separate B+ tree within the database.
var (
	// BucketMetadataIndex is the inverted index for metadata.
	// Key: [ledgerName\x00][ns:][metadataKey\x00][typeTag][sortableValue][entityID]
	// Value: (empty)
	BucketMetadataIndex = []byte("midx")

	// BucketExistence is the existence index for accounts and transactions.
	// Key: [ledgerName\x00][ns:][entityID]
	// Value: (empty)
	BucketExistence = []byte("exist")

	// BucketReverseMap is the reverse metadata map (entity → current value per key).
	// Key: [ledgerName\x00][ns:][entityID separator][metadataKey]
	//   accounts:     [ledger\x00][a:][account\x00][key]
	//   transactions: [ledger\x00][t:][txID(8B)][key]
	// Value: MetadataValue protobuf
	BucketReverseMap = []byte("rmap")

	// BucketAccountTx maps accounts to their transactions (any role: source or destination).
	// Key: [ledgerName\x00][accountAddress\x00][txID(8B)]
	// Value: (empty)
	BucketAccountTx = []byte("atxm")

	// BucketSourceAccountTx maps source accounts to their transactions.
	// Key format is identical to BucketAccountTx.
	BucketSourceAccountTx = []byte("satx")

	// BucketDestAccountTx maps destination accounts to their transactions.
	// Key format is identical to BucketAccountTx.
	BucketDestAccountTx = []byte("datx")

	// BucketEntityExists is the entity-ordered existence index for metadata keys.
	// It maps (ledger, namespace, metadataKey) → sorted entity IDs, partitioned
	// by null status. This enables streaming ExistsCondition queries via PrefixIterator.
	// Key: [ledgerName\x00][ns:][metadataKey\x00][nullFlag(1B)][entityID]
	//   nullFlag = 0x00 → non-null (typed metadata: string, int, uint, bool)
	//   nullFlag = 0x01 → null (TypeTagNull — unconvertible or explicit null)
	// Value: (empty)
	BucketEntityExists = []byte("eidx")

	// BucketProgress stores index builder progress.
	// Key: "lastSeq"
	// Value: uint64 big-endian
	BucketProgress = []byte("prog")
)

// Namespace prefixes to distinguish accounts and transactions in shared buckets.
const (
	NamespaceAccount     = "a:"
	NamespaceTransaction = "t:"
)

// KeyBuilder constructs keys for bbolt buckets.
// It reuses a single byte slice to minimize allocations.
type KeyBuilder struct {
	buf []byte
}

// NewKeyBuilder creates a new KeyBuilder with preallocated capacity.
func NewKeyBuilder() *KeyBuilder {
	return &KeyBuilder{
		buf: make([]byte, 0, 256),
	}
}

// Reset clears the builder for reuse.
func (kb *KeyBuilder) Reset() *KeyBuilder {
	kb.buf = kb.buf[:0]
	return kb
}

// PutLedger appends a ledger name followed by a null terminator.
func (kb *KeyBuilder) PutLedger(name string) *KeyBuilder {
	kb.buf = append(kb.buf, name...)
	kb.buf = append(kb.buf, 0x00)
	return kb
}

// PutNamespace appends a namespace prefix (e.g., "a:" or "t:").
func (kb *KeyBuilder) PutNamespace(ns string) *KeyBuilder {
	kb.buf = append(kb.buf, ns...)
	return kb
}

// PutString appends a raw string.
func (kb *KeyBuilder) PutString(s string) *KeyBuilder {
	kb.buf = append(kb.buf, s...)
	return kb
}

// PutStringNull appends a string followed by a null terminator.
func (kb *KeyBuilder) PutStringNull(s string) *KeyBuilder {
	kb.buf = append(kb.buf, s...)
	kb.buf = append(kb.buf, 0x00)
	return kb
}

// PutBytes appends raw bytes.
func (kb *KeyBuilder) PutBytes(b []byte) *KeyBuilder {
	kb.buf = append(kb.buf, b...)
	return kb
}

// PutByte appends a single byte.
func (kb *KeyBuilder) PutByte(b byte) *KeyBuilder {
	kb.buf = append(kb.buf, b)
	return kb
}

// PutUint64 appends a uint64 in big-endian order.
func (kb *KeyBuilder) PutUint64(v uint64) *KeyBuilder {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	kb.buf = append(kb.buf, buf[:]...)
	return kb
}

// Build returns a copy of the constructed key and resets the builder.
func (kb *KeyBuilder) Build() []byte {
	result := make([]byte, len(kb.buf))
	copy(result, kb.buf)
	kb.buf = kb.buf[:0]
	return result
}

// Snapshot returns a copy of the current key state without resetting.
func (kb *KeyBuilder) Snapshot() []byte {
	result := make([]byte, len(kb.buf))
	copy(result, kb.buf)
	return result
}

// Len returns the current length of the key being built.
func (kb *KeyBuilder) Len() int {
	return len(kb.buf)
}

// MetadataIndexPrefix returns the prefix for scanning all entries of a specific
// metadata key within a namespace. Used for ExistsCondition and schema change handling.
//
//	[ledgerName\x00][ns:][metadataKey\x00]
func MetadataIndexPrefix(kb *KeyBuilder, ledger, ns, metadataKey string) []byte {
	return kb.Reset().
		PutLedger(ledger).
		PutNamespace(ns).
		PutStringNull(metadataKey).
		Snapshot()
}

// MetadataIndexKey builds a full metadata inverted index key.
//
//	[ledgerName\x00][ns:][metadataKey\x00][typeTag+sortableValue][entityID]
func MetadataIndexKey(kb *KeyBuilder, ledger, ns, metadataKey string, encodedValue []byte, entityID []byte) []byte {
	return kb.Reset().
		PutLedger(ledger).
		PutNamespace(ns).
		PutStringNull(metadataKey).
		PutBytes(encodedValue).
		PutBytes(entityID).
		Build()
}

// ExistenceKey builds an existence index key.
//
//	[ledgerName\x00][ns:][entityID]
func ExistenceKey(kb *KeyBuilder, ledger, ns string, entityID []byte) []byte {
	return kb.Reset().
		PutLedger(ledger).
		PutNamespace(ns).
		PutBytes(entityID).
		Build()
}

// ExistencePrefix returns the prefix for scanning all entities in a namespace.
//
//	[ledgerName\x00][ns:]
func ExistencePrefix(kb *KeyBuilder, ledger, ns string) []byte {
	return kb.Reset().
		PutLedger(ledger).
		PutNamespace(ns).
		Snapshot()
}

// AccountReverseMapKey builds a reverse map key for account metadata.
//
//	[ledgerName\x00][a:][account\x00][metadataKey]
func AccountReverseMapKey(kb *KeyBuilder, ledger, account, metadataKey string) []byte {
	return kb.Reset().
		PutLedger(ledger).
		PutNamespace(NamespaceAccount).
		PutStringNull(account).
		PutString(metadataKey).
		Build()
}

// TransactionReverseMapKey builds a reverse map key for transaction metadata.
//
//	[ledgerName\x00][t:][txID(8B)][metadataKey]
func TransactionReverseMapKey(kb *KeyBuilder, ledger string, txID uint64, metadataKey string) []byte {
	return kb.Reset().
		PutLedger(ledger).
		PutNamespace(NamespaceTransaction).
		PutUint64(txID).
		PutString(metadataKey).
		Build()
}

// AccountTxKey builds an account-to-transaction mapping key.
//
//	[ledgerName\x00][accountAddress\x00][txID(8B)]
func AccountTxKey(kb *KeyBuilder, ledger, account string, txID uint64) []byte {
	return kb.Reset().
		PutLedger(ledger).
		PutStringNull(account).
		PutUint64(txID).
		Build()
}

// AccountTxPrefix returns the prefix for scanning all transactions for an account.
//
//	[ledgerName\x00][accountAddress\x00]
func AccountTxPrefix(kb *KeyBuilder, ledger, account string) []byte {
	return kb.Reset().
		PutLedger(ledger).
		PutStringNull(account).
		Snapshot()
}

// Null flag bytes for the entity-ordered existence index (eidx).
const (
	EntityExistsNonNull byte = 0x00
	EntityExistsNull    byte = 0x01
)

// EntityExistsKey builds a full entity-ordered existence index key.
//
//	[ledgerName\x00][ns:][metadataKey\x00][nullFlag][entityID]
func EntityExistsKey(kb *KeyBuilder, ledger, ns, metaKey string, isNull bool, entityID []byte) []byte {
	nullFlag := EntityExistsNonNull
	if isNull {
		nullFlag = EntityExistsNull
	}
	return kb.Reset().
		PutLedger(ledger).
		PutNamespace(ns).
		PutStringNull(metaKey).
		PutByte(nullFlag).
		PutBytes(entityID).
		Build()
}

// EntityExistsNonNullPrefix returns the prefix for scanning non-null entities
// that have a given metadata key.
//
//	[ledgerName\x00][ns:][metadataKey\x00][0x00]
func EntityExistsNonNullPrefix(kb *KeyBuilder, ledger, ns, metaKey string) []byte {
	return kb.Reset().
		PutLedger(ledger).
		PutNamespace(ns).
		PutStringNull(metaKey).
		PutByte(EntityExistsNonNull).
		Snapshot()
}

// EntityExistsNullPrefix returns the prefix for scanning null-valued entities
// that have a given metadata key.
//
//	[ledgerName\x00][ns:][metadataKey\x00][0x01]
func EntityExistsNullPrefix(kb *KeyBuilder, ledger, ns, metaKey string) []byte {
	return kb.Reset().
		PutLedger(ledger).
		PutNamespace(ns).
		PutStringNull(metaKey).
		PutByte(EntityExistsNull).
		Snapshot()
}
