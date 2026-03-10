package readstore

import "github.com/formancehq/ledger-v3-poc/internal/storage/dal"

// Bucket names for bbolt. Each bucket is a separate B+ tree within the database.
var (
	// BucketMetadataIndex is the inverted index for metadata.
	// Key: [ledgerName\x00][ns:][metadataKey\x00][typeTag][sortableValue][entityID]
	// Value: (empty).
	BucketMetadataIndex = []byte("midx")

	// BucketReverseMap is the reverse metadata map (entity → current value per key).
	// Key: [ledgerName\x00][ns:][entityID separator][metadataKey]
	//   accounts:     [ledger\x00][a:][account\x00][key]
	//   transactions: [ledger\x00][t:][txID(8B)][key]
	// Value: MetadataValue protobuf.
	BucketReverseMap = []byte("rmap")

	// BucketAccountTx maps accounts to their transactions (any role: source or destination).
	// Key: [ledgerName\x00][accountAddress\x00][txID(8B)]
	// Value: (empty).
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
	// Value: (empty).
	BucketEntityExists = []byte("eidx")

	// BucketProgress stores index builder progress.
	// Keys: "lastSeq" (last indexed log sequence), "lastRaftIdx" (last indexed raft index).
	// Value: uint64 big-endian.
	BucketProgress = []byte("prog")

	// BucketBackfill stores per-index backfill progress cursors.
	// Key: [ledger\x00][kind_byte][details]
	//   TxBuiltin:    [ledger\x00]b[builtin_byte]
	//   TxMetadata:   [ledger\x00]T[key]
	//   AcctBuiltin:  [ledger\x00]A[builtin_byte]
	//   AcctMetadata: [ledger\x00]a[key]
	//   LogBuiltin:   [ledger\x00]l[builtin_byte]
	// Value: uint64 big-endian (cursor position).
	BucketBackfill = []byte("bfil")

	// BucketTransactionReference maps (ledger, reference) → txID for exact-match lookups.
	// Key: [ledger\x00][reference\x00][txID_BE(8B)]
	// Value: (empty).
	BucketTransactionReference = []byte("txref")

	// BucketTransactionTimestamp maps (ledger, timestamp, txID) for range scans by timestamp.
	// Key: [ledger\x00][timestamp_BE(8B)][txID_BE(8B)]
	// Value: (empty).
	BucketTransactionTimestamp = []byte("tstmp")

	// BucketLedgerLogs maps (ledger, ledgerLogID) → globalSequence for per-ledger log listing.
	// Key: [ledger\x00][ledgerLogID_BE(8B)]
	// Value: [globalSequence_BE(8B)].
	BucketLedgerLogs = []byte("llog")

	// BucketLedgerLogDate maps (ledger, timestamp, logID) for range scans by log date.
	// Key: [ledger\x00][timestamp_BE(8B)][logID_BE(8B)]
	// Value: (empty).
	BucketLedgerLogDate = []byte("lldt")
)

// Namespace prefixes to distinguish accounts, transactions, and logs in shared buckets.
const (
	NamespaceAccount     = "a:"
	NamespaceTransaction = "t:"
	NamespaceLog         = "l:"
)

// Null flag bytes for the entity-ordered existence index (eidx).
const (
	EntityExistsNonNull byte = 0x00
	EntityExistsNull    byte = 0x01
)

// Backfill key kind bytes identify the index type in a bbolt backfill progress key.
const (
	BackfillKindTxBuiltin    = byte('b') // builtin transaction field index: [ledger\x00]b[builtin_byte]
	BackfillKindTxMetadata   = byte('T') // transaction metadata index: [ledger\x00]T[key]
	BackfillKindAcctBuiltin  = byte('A') // builtin account field index: [ledger\x00]A[builtin_byte]
	BackfillKindAcctMetadata = byte('a') // account metadata index: [ledger\x00]a[key]
	BackfillKindLogBuiltin     = byte('l') // builtin log field index: [ledger\x00]l[builtin_byte]
	BackfillKindSchemaRewrite = byte('S') // schema rewrite task: [ledger\x00]S[targetType_byte][key]
)

// ParseBackfillKey decodes a bbolt backfill key into its components.
// Format:
//
//	TxBuiltin:    [ledger\x00]b[builtin_byte]
//	TxMetadata:   [ledger\x00]T[key]
//	AcctBuiltin:  [ledger\x00]A[builtin_byte]
//	AcctMetadata: [ledger\x00]a[key]
//	LogBuiltin:   [ledger\x00]l[builtin_byte]
//
// Returns the ledger name, kind byte, remaining details, and ok.
func ParseBackfillKey(key []byte) (ledger string, kind byte, details []byte, ok bool) {
	// Find the null separator between ledger name and type indicator.
	for i, b := range key {
		if b == 0x00 {
			if i+1 >= len(key) {
				return "", 0, nil, false
			}

			return string(key[:i]), key[i+1], key[i+2:], true
		}
	}

	return "", 0, nil, false
}

// MetadataIndexPrefix returns the prefix for scanning all entries of a specific
// metadata key within a namespace. Used for ExistsCondition and schema change handling.
//
//	[ledgerName\x00][ns:][metadataKey\x00]
func MetadataIndexPrefix(kb *dal.KeyBuilder, ledger, ns, metadataKey string) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		PutNamespace(ns).
		PutStringNull(metadataKey).
		Snapshot()
}

// MetadataIndexKey builds a full metadata inverted index key.
//
//	[ledgerName\x00][ns:][metadataKey\x00][typeTag+sortableValue][entityID]
func MetadataIndexKey(kb *dal.KeyBuilder, ledger, ns, metadataKey string, encodedValue []byte, entityID []byte) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		PutNamespace(ns).
		PutStringNull(metadataKey).
		PutBytes(encodedValue).
		PutBytes(entityID).
		Build()
}

// AccountReverseMapKey builds a reverse map key for account metadata.
//
//	[ledgerName\x00][a:][account\x00][metadataKey]
func AccountReverseMapKey(kb *dal.KeyBuilder, ledger, account, metadataKey string) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		PutNamespace(NamespaceAccount).
		PutStringNull(account).
		PutString(metadataKey).
		Build()
}

// TransactionReverseMapKey builds a reverse map key for transaction metadata.
//
//	[ledgerName\x00][t:][txID(8B)][metadataKey]
func TransactionReverseMapKey(kb *dal.KeyBuilder, ledger string, txID uint64, metadataKey string) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		PutNamespace(NamespaceTransaction).
		PutUint64(txID).
		PutString(metadataKey).
		Build()
}

// AccountTxKey builds an account-to-transaction mapping key.
//
//	[ledgerName\x00][accountAddress\x00][txID(8B)]
func AccountTxKey(kb *dal.KeyBuilder, ledger, account string, txID uint64) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		PutStringNull(account).
		PutUint64(txID).
		Build()
}

// AccountTxPrefix returns the prefix for scanning all transactions for an account.
//
//	[ledgerName\x00][accountAddress\x00]
func AccountTxPrefix(kb *dal.KeyBuilder, ledger, account string) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		PutStringNull(account).
		Snapshot()
}

// EntityExistsKey builds a full entity-ordered existence index key.
//
//	[ledgerName\x00][ns:][metadataKey\x00][nullFlag][entityID]
func EntityExistsKey(kb *dal.KeyBuilder, ledger, ns, metaKey string, isNull bool, entityID []byte) []byte {
	nullFlag := EntityExistsNonNull
	if isNull {
		nullFlag = EntityExistsNull
	}

	return kb.Reset().
		PutLedgerName(ledger).
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
func EntityExistsNonNullPrefix(kb *dal.KeyBuilder, ledger, ns, metaKey string) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		PutNamespace(ns).
		PutStringNull(metaKey).
		PutByte(EntityExistsNonNull).
		Snapshot()
}

// EntityExistsNullPrefix returns the prefix for scanning null-valued entities
// that have a given metadata key.
//
//	[ledgerName\x00][ns:][metadataKey\x00][0x01]
func EntityExistsNullPrefix(kb *dal.KeyBuilder, ledger, ns, metaKey string) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		PutNamespace(ns).
		PutStringNull(metaKey).
		PutByte(EntityExistsNull).
		Snapshot()
}

// TransactionReferenceKey builds a full key in BucketTransactionReference.
//
//	[ledger\x00][reference\x00][txID_BE(8B)]
func TransactionReferenceKey(kb *dal.KeyBuilder, ledger, reference string, txID uint64) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		PutStringNull(reference).
		PutUint64(txID).
		Build()
}

// TransactionReferencePrefix returns the prefix for scanning all txIDs with a given reference.
//
//	[ledger\x00][reference\x00]
func TransactionReferencePrefix(kb *dal.KeyBuilder, ledger, reference string) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		PutStringNull(reference).
		Snapshot()
}

// TransactionTimestampKey builds a full key in BucketTransactionTimestamp.
//
//	[ledger\x00][timestamp_BE(8B)][txID_BE(8B)]
func TransactionTimestampKey(kb *dal.KeyBuilder, ledger string, timestamp, txID uint64) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		PutUint64(timestamp).
		PutUint64(txID).
		Build()
}

// TransactionTimestampRangePrefix returns the ledger prefix for range scans in BucketTransactionTimestamp.
//
//	[ledger\x00]
func TransactionTimestampRangePrefix(kb *dal.KeyBuilder, ledger string) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		Snapshot()
}

// LedgerLogKey builds a full key in BucketLedgerLogs.
//
//	[ledger\x00][ledgerLogID_BE(8B)]
func LedgerLogKey(kb *dal.KeyBuilder, ledger string, logID uint64) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		PutUint64(logID).
		Build()
}

// LedgerLogPrefix returns the ledger prefix for range scans in BucketLedgerLogs.
//
//	[ledger\x00]
func LedgerLogPrefix(kb *dal.KeyBuilder, ledger string) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		Snapshot()
}

// LedgerLogDateKey builds a full key in BucketLedgerLogDate.
//
//	[ledger\x00][timestamp_BE(8B)][logID_BE(8B)]
func LedgerLogDateKey(kb *dal.KeyBuilder, ledger string, timestamp, logID uint64) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		PutUint64(timestamp).
		PutUint64(logID).
		Build()
}

// LedgerLogDateRangePrefix returns the ledger prefix for range scans in BucketLedgerLogDate.
//
//	[ledger\x00]
func LedgerLogDateRangePrefix(kb *dal.KeyBuilder, ledger string) []byte {
	return kb.Reset().
		PutLedgerName(ledger).
		Snapshot()
}
