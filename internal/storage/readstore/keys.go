package readstore

import "github.com/formancehq/ledger-v3-poc/internal/storage/dal"

// Pebble key prefix bytes for the separate read index database.
// Each prefix replaces a former Pebble bucket.
const (
	PrefixMetadataIndex         byte = 0x01 // midx — inverted index for metadata
	PrefixEntityExists          byte = 0x02 // eidx — entity-ordered existence index
	PrefixReverseMap            byte = 0x03 // rmap — reverse metadata map
	PrefixAccountTx             byte = 0x04 // atxm — account→tx (any role)
	PrefixSourceAccountTx       byte = 0x05 // satx — source account→tx
	PrefixDestAccountTx         byte = 0x06 // datx — dest account→tx
	PrefixTransactionReference  byte = 0x07 // txref — transaction reference
	PrefixTransactionTimestamp  byte = 0x08 // tstmp — transaction timestamp
	PrefixLedgerLogs            byte = 0x09 // llog — ledger log mapping
	PrefixLedgerLogDate         byte = 0x0A // lldt — ledger log date
	PrefixTransactionInsertedAt byte = 0x0B // txiat — transaction inserted_at
	PrefixProgress              byte = 0xF0 // prog — progress (single key)
	PrefixBackfill              byte = 0xF1 // bfil — backfill cursors
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

// Backfill key kind bytes identify the index type in a backfill progress key.
const (
	BackfillKindTxBuiltin     = byte('b') // builtin transaction field index: [ledger\x00]b[builtin_byte]
	BackfillKindTxMetadata    = byte('T') // transaction metadata index: [ledger\x00]T[key]
	BackfillKindAcctBuiltin   = byte('A') // builtin account field index: [ledger\x00]A[builtin_byte]
	BackfillKindAcctMetadata  = byte('a') // account metadata index: [ledger\x00]a[key]
	BackfillKindLogBuiltin    = byte('l') // builtin log field index: [ledger\x00]l[builtin_byte]
	BackfillKindSchemaRewrite = byte('S') // schema rewrite task: [ledger\x00]S[targetType_byte][key]
)

// ParseBackfillKey decodes a backfill key into its components.
// The key does NOT include the PrefixBackfill byte — that is stripped by the caller.
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
//	[0x01][ledgerName\x00][ns:][metadataKey\x00]
func MetadataIndexPrefix(kb *dal.KeyBuilder, ledger, ns, metadataKey string) []byte {
	return kb.Reset().
		PutByte(PrefixMetadataIndex).
		PutLedgerName(ledger).
		PutNamespace(ns).
		PutStringNull(metadataKey).
		Snapshot()
}

// MetadataIndexKey builds a full metadata inverted index key.
//
//	[0x01][ledgerName\x00][ns:][metadataKey\x00][typeTag+sortableValue][entityID]
func MetadataIndexKey(kb *dal.KeyBuilder, ledger, ns, metadataKey string, encodedValue []byte, entityID []byte) []byte {
	return kb.Reset().
		PutByte(PrefixMetadataIndex).
		PutLedgerName(ledger).
		PutNamespace(ns).
		PutStringNull(metadataKey).
		PutBytes(encodedValue).
		PutBytes(entityID).
		Build()
}

// AccountReverseMapKey builds a reverse map key for account metadata.
//
//	[0x03][ledgerName\x00][a:][account\x00][metadataKey]
func AccountReverseMapKey(kb *dal.KeyBuilder, ledger, account, metadataKey string) []byte {
	return kb.Reset().
		PutByte(PrefixReverseMap).
		PutLedgerName(ledger).
		PutNamespace(NamespaceAccount).
		PutStringNull(account).
		PutString(metadataKey).
		Build()
}

// TransactionReverseMapKey builds a reverse map key for transaction metadata.
//
//	[0x03][ledgerName\x00][t:][txID(8B)][metadataKey]
func TransactionReverseMapKey(kb *dal.KeyBuilder, ledger string, txID uint64, metadataKey string) []byte {
	return kb.Reset().
		PutByte(PrefixReverseMap).
		PutLedgerName(ledger).
		PutNamespace(NamespaceTransaction).
		PutUint64(txID).
		PutString(metadataKey).
		Build()
}

// AccountTxKey builds an account-to-transaction mapping key.
//
//	[prefix][ledgerName\x00][accountAddress\x00][txID(8B)]
func AccountTxKey(kb *dal.KeyBuilder, prefix byte, ledger, account string, txID uint64) []byte {
	return kb.Reset().
		PutByte(prefix).
		PutLedgerName(ledger).
		PutStringNull(account).
		PutUint64(txID).
		Build()
}

// AccountTxPrefix returns the prefix for scanning all transactions for an account.
//
//	[prefix][ledgerName\x00][accountAddress\x00]
func AccountTxPrefix(kb *dal.KeyBuilder, prefix byte, ledger, account string) []byte {
	return kb.Reset().
		PutByte(prefix).
		PutLedgerName(ledger).
		PutStringNull(account).
		Snapshot()
}

// EntityExistsKey builds a full entity-ordered existence index key.
//
//	[0x02][ledgerName\x00][ns:][metadataKey\x00][nullFlag][entityID]
func EntityExistsKey(kb *dal.KeyBuilder, ledger, ns, metaKey string, isNull bool, entityID []byte) []byte {
	nullFlag := EntityExistsNonNull
	if isNull {
		nullFlag = EntityExistsNull
	}

	return kb.Reset().
		PutByte(PrefixEntityExists).
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
//	[0x02][ledgerName\x00][ns:][metadataKey\x00][0x00]
func EntityExistsNonNullPrefix(kb *dal.KeyBuilder, ledger, ns, metaKey string) []byte {
	return kb.Reset().
		PutByte(PrefixEntityExists).
		PutLedgerName(ledger).
		PutNamespace(ns).
		PutStringNull(metaKey).
		PutByte(EntityExistsNonNull).
		Snapshot()
}

// EntityExistsNullPrefix returns the prefix for scanning null-valued entities
// that have a given metadata key.
//
//	[0x02][ledgerName\x00][ns:][metadataKey\x00][0x01]
func EntityExistsNullPrefix(kb *dal.KeyBuilder, ledger, ns, metaKey string) []byte {
	return kb.Reset().
		PutByte(PrefixEntityExists).
		PutLedgerName(ledger).
		PutNamespace(ns).
		PutStringNull(metaKey).
		PutByte(EntityExistsNull).
		Snapshot()
}

// TransactionReferenceKey builds a full key in the transaction reference index.
//
//	[0x07][ledger\x00][reference\x00][txID_BE(8B)]
func TransactionReferenceKey(kb *dal.KeyBuilder, ledger, reference string, txID uint64) []byte {
	return kb.Reset().
		PutByte(PrefixTransactionReference).
		PutLedgerName(ledger).
		PutStringNull(reference).
		PutUint64(txID).
		Build()
}

// TransactionReferencePrefix returns the prefix for scanning all txIDs with a given reference.
//
//	[0x07][ledger\x00][reference\x00]
func TransactionReferencePrefix(kb *dal.KeyBuilder, ledger, reference string) []byte {
	return kb.Reset().
		PutByte(PrefixTransactionReference).
		PutLedgerName(ledger).
		PutStringNull(reference).
		Snapshot()
}

// TransactionTimestampKey builds a full key in the transaction timestamp index.
//
//	[0x08][ledger\x00][timestamp_BE(8B)][txID_BE(8B)]
func TransactionTimestampKey(kb *dal.KeyBuilder, ledger string, timestamp, txID uint64) []byte {
	return kb.Reset().
		PutByte(PrefixTransactionTimestamp).
		PutLedgerName(ledger).
		PutUint64(timestamp).
		PutUint64(txID).
		Build()
}

// TransactionTimestampRangePrefix returns the ledger prefix for range scans in the timestamp index.
//
//	[0x08][ledger\x00]
func TransactionTimestampRangePrefix(kb *dal.KeyBuilder, ledger string) []byte {
	return kb.Reset().
		PutByte(PrefixTransactionTimestamp).
		PutLedgerName(ledger).
		Snapshot()
}

// TransactionInsertedAtKey builds a full key in the transaction inserted_at index.
//
//	[0x0B][ledger\x00][timestamp_BE(8B)][txID_BE(8B)]
func TransactionInsertedAtKey(kb *dal.KeyBuilder, ledger string, timestamp, txID uint64) []byte {
	return kb.Reset().
		PutByte(PrefixTransactionInsertedAt).
		PutLedgerName(ledger).
		PutUint64(timestamp).
		PutUint64(txID).
		Build()
}

// TransactionInsertedAtRangePrefix returns the ledger prefix for range scans in the inserted_at index.
//
//	[0x0B][ledger\x00]
func TransactionInsertedAtRangePrefix(kb *dal.KeyBuilder, ledger string) []byte {
	return kb.Reset().
		PutByte(PrefixTransactionInsertedAt).
		PutLedgerName(ledger).
		Snapshot()
}

// LedgerLogKey builds a full key in the ledger logs index.
//
//	[0x09][ledger\x00][ledgerLogID_BE(8B)]
func LedgerLogKey(kb *dal.KeyBuilder, ledger string, logID uint64) []byte {
	return kb.Reset().
		PutByte(PrefixLedgerLogs).
		PutLedgerName(ledger).
		PutUint64(logID).
		Build()
}

// LedgerLogPrefix returns the ledger prefix for range scans in the ledger logs index.
//
//	[0x09][ledger\x00]
func LedgerLogPrefix(kb *dal.KeyBuilder, ledger string) []byte {
	return kb.Reset().
		PutByte(PrefixLedgerLogs).
		PutLedgerName(ledger).
		Snapshot()
}

// LedgerLogDateKey builds a full key in the ledger log date index.
//
//	[0x0A][ledger\x00][timestamp_BE(8B)][logID_BE(8B)]
func LedgerLogDateKey(kb *dal.KeyBuilder, ledger string, timestamp, logID uint64) []byte {
	return kb.Reset().
		PutByte(PrefixLedgerLogDate).
		PutLedgerName(ledger).
		PutUint64(timestamp).
		PutUint64(logID).
		Build()
}

// LedgerLogDateRangePrefix returns the ledger prefix for range scans in the log date index.
//
//	[0x0A][ledger\x00]
func LedgerLogDateRangePrefix(kb *dal.KeyBuilder, ledger string) []byte {
	return kb.Reset().
		PutByte(PrefixLedgerLogDate).
		PutLedgerName(ledger).
		Snapshot()
}

// ReverseMapPrefix returns the prefix for scanning reverse map entries
// within a namespace.
//
//	[0x03][ledgerName\x00][ns:]
func ReverseMapPrefix(kb *dal.KeyBuilder, ledger, ns string) []byte {
	return kb.Reset().
		PutByte(PrefixReverseMap).
		PutLedgerName(ledger).
		PutNamespace(ns).
		Snapshot()
}

// BackfillKeyPrefix returns the prefix byte for backfill keys.
//
//	[0xF1]
func BackfillKeyPrefix() []byte {
	return []byte{PrefixBackfill}
}

// ProgressKey returns the full key for the progress entry.
//
//	[0xF0]lastSeq
func ProgressKey() []byte {
	key := make([]byte, 0, 8)
	key = append(key, PrefixProgress)
	key = append(key, "lastSeq"...)

	return key
}
