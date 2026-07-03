package readstore

import (
	"bytes"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Pebble key prefix bytes for the separate read index database.
// Each prefix replaces a former Pebble bucket.
const (
	PrefixMetadataIndex         byte = 0x01 // midx — inverted index for metadata
	PrefixEntityExists          byte = 0x02 // eidx — entity-ordered existence index
	PrefixReverseMap            byte = 0x03 // rmap — reverse metadata map
	PrefixAccountTx             byte = 0x04 // atxm — account→tx (any role)
	PrefixSourceAccountTx       byte = 0x05 // satx — source account→tx
	PrefixDestinationAccountTx  byte = 0x06 // datx — destination account→tx
	PrefixTransactionReference  byte = 0x07 // txref — transaction reference
	PrefixTransactionTimestamp  byte = 0x08 // tstmp — transaction timestamp
	PrefixLedgerLogs            byte = 0x09 // llog — ledger log mapping
	PrefixLedgerLogDate         byte = 0x0A // lldt — ledger log date
	PrefixTransactionInsertedAt byte = 0x0B // txiat — transaction inserted_at
	PrefixAccountByAsset        byte = 0x0C // abya — account-by-asset inverted index (asset→account)

	// PrefixInternal groups all non-ledger-scoped keys under a single prefix
	// so that Comparer.Split can treat them uniformly (full key = prefix).
	// New internal keys should be added as sub-prefixes here.
	PrefixInternal                     byte = 0xFE
	SubInternalProgress                byte = 0x01 // [0xFE][0x01] — last indexed log sequence
	SubInternalAppliedProposalProgress byte = 0x02 // [0xFE][0x02] — last consumed AppliedProposal sequence
	SubInternalBackfill                byte = 0x03 // [0xFE][0x03][ledgerName padded 64B][kind][...] — backfill cursors
	// SubInternalIndexVersion stores the per-replica (current_version,
	// pending_version, rewrite_progress) for each indexed metadata
	// field. Key shape: [0xFE][0x04][ledgerName padded 64B][indexID canonical].
	SubInternalIndexVersion byte = 0x04

	// SubInternalAuditIndex is the keyspace for the audit secondary index.
	// Layout: [PrefixInternal][SubInternalAuditIndex][AuditField][value][audit_seq BE8] -> ∅.
	SubInternalAuditIndex byte = 0x05
	// SubInternalAuditProgress holds the per-replica audit indexing cursor.
	// Layout: [PrefixInternal][SubInternalAuditProgress] -> last_indexed_audit_sequence BE8.
	SubInternalAuditProgress byte = 0x06
)

// AuditField discriminates the indexed field within the audit-index keyspace.
const (
	AuditFieldOutcome       byte = 0x01 // 1 byte value: 0=failure, 1=success
	AuditFieldLedger        byte = 0x02 // string value (match-any over AuditEntry.Ledgers)
	AuditFieldCallerSubject byte = 0x03 // string value
	AuditFieldOrderType     byte = 0x04 // string token (match-any over items)
	AuditFieldTimestamp     byte = 0x05 // BE uint64 raw HLC Timestamp.Data (unix microseconds) (range)
	AuditFieldProposalID    byte = 0x06 // BE uint64 (range)
	AuditFieldLogSeq        byte = 0x07 // BE uint64 (range, match-any over items)
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
	BackfillKindTxBuiltin     = byte('b') // builtin transaction field index: [ledgerName padded 64B]b[builtin_byte]
	BackfillKindTxMetadata    = byte('T') // transaction metadata index: [ledgerName padded 64B]T[key]
	BackfillKindAcctBuiltin   = byte('A') // builtin account field index: [ledgerName padded 64B]A[builtin_byte]
	BackfillKindAcctMetadata  = byte('a') // account metadata index: [ledgerName padded 64B]a[key]
	BackfillKindLogBuiltin    = byte('l') // builtin log field index: [ledgerName padded 64B]l[builtin_byte]
	BackfillKindSchemaRewrite = byte('S') // schema rewrite task: [ledgerName padded 64B]S[targetType_byte][key]
)

// ParseBackfillKey decodes a backfill key into its components.
// The key does NOT include the PrefixBackfill bytes — that prefix is stripped by the caller.
// Format:
//
//	TxBuiltin:    [ledgerName padded 64B]b[builtin_byte]
//	TxMetadata:   [ledgerName padded 64B]T[key]
//	AcctBuiltin:  [ledgerName padded 64B]A[builtin_byte]
//	AcctMetadata: [ledgerName padded 64B]a[key]
//	LogBuiltin:   [ledgerName padded 64B]l[builtin_byte]
//
// Returns the ledger name (trimmed of zero padding), kind byte, remaining details, and ok.
func ParseBackfillKey(key []byte) (ledgerName string, kind byte, details []byte, ok bool) {
	// Need at least LedgerNameFixedSize bytes for the name + 1 byte for kind.
	if len(key) < dal.LedgerNameFixedSize+1 {
		return "", 0, nil, false
	}

	raw := key[:dal.LedgerNameFixedSize]

	end := bytes.IndexByte(raw, 0)
	if end < 0 {
		end = dal.LedgerNameFixedSize
	}

	return string(raw[:end]), key[dal.LedgerNameFixedSize], key[dal.LedgerNameFixedSize+1:], true
}

// MetadataIndexPrefix returns the prefix for scanning all entries of a
// specific metadata key within a namespace. Used for ExistsCondition
// and schema change handling. Equivalent to MetadataIndexPrefixV with
// version 1 — kept for callers not yet aware of versioning.
//
//	[0x01][ledgerName padded 64B][ns:][metadataKey\x00][version:4B BE = 1]
func MetadataIndexPrefix(kb *dal.KeyBuilder, ledgerName string, ns, metadataKey string) []byte {
	return MetadataIndexPrefixV(kb, ledgerName, ns, metadataKey, 1)
}

// MetadataIndexPrefixV is the version-aware variant of MetadataIndexPrefix.
//
//	[0x01][ledgerName padded 64B][ns:][metadataKey\x00][version:4B BE]
func MetadataIndexPrefixV(kb *dal.KeyBuilder, ledgerName string, ns, metadataKey string, version uint32) []byte {
	return kb.Reset().
		PutByte(PrefixMetadataIndex).
		PutLedgerNameFixed(ledgerName).
		PutNamespace(ns).
		PutStringNull(metadataKey).
		PutUint32(version).
		Snapshot()
}

// MetadataIndexKey builds a full metadata inverted index key under
// version 1. Equivalent to MetadataIndexKeyV(..., 1, ...) — kept for
// callers not yet aware of versioning.
func MetadataIndexKey(kb *dal.KeyBuilder, ledgerName string, ns, metadataKey string, encodedValue []byte, entityID []byte) []byte {
	return MetadataIndexKeyV(kb, ledgerName, ns, metadataKey, 1, encodedValue, entityID)
}

// MetadataIndexKeyV builds a full metadata inverted index key. Forward-
// encoding version sits between the metadata key separator and the
// encoded value so that each version has its own contiguous scan range
// (see MetadataIndexPrefixV).
//
//	[0x01][ledgerName padded 64B][ns:][metadataKey\x00][version:4B BE][typeTag+sortableValue][entityID]
func MetadataIndexKeyV(kb *dal.KeyBuilder, ledgerName string, ns, metadataKey string, version uint32, encodedValue []byte, entityID []byte) []byte {
	return kb.Reset().
		PutByte(PrefixMetadataIndex).
		PutLedgerNameFixed(ledgerName).
		PutNamespace(ns).
		PutStringNull(metadataKey).
		PutUint32(version).
		PutBytes(encodedValue).
		PutBytes(entityID).
		Consume()
}

// AccountReverseMapKey builds a reverse map key for account metadata
// under version 1. Equivalent to AccountReverseMapKeyV(..., 1).
func AccountReverseMapKey(kb *dal.KeyBuilder, ledgerName string, account, metadataKey string) []byte {
	return AccountReverseMapKeyV(kb, ledgerName, account, metadataKey, 1)
}

// AccountReverseMapKeyV builds a reverse map key for account metadata.
// Forward-encoding version is encoded fixed-width *before* the metadata
// key so the metaKey suffix scan (purgeReverseMapForKey) stays
// parseable.
//
//	[0x03][ledgerName padded 64B][a:][account\x00][version:4B BE][metadataKey]
func AccountReverseMapKeyV(kb *dal.KeyBuilder, ledgerName string, account, metadataKey string, version uint32) []byte {
	return kb.Reset().
		PutByte(PrefixReverseMap).
		PutLedgerNameFixed(ledgerName).
		PutNamespace(NamespaceAccount).
		PutStringNull(account).
		PutUint32(version).
		PutString(metadataKey).
		Build()
}

// TransactionReverseMapKey builds a reverse map key for transaction
// metadata under version 1.
func TransactionReverseMapKey(kb *dal.KeyBuilder, ledgerName string, txID uint64, metadataKey string) []byte {
	return TransactionReverseMapKeyV(kb, ledgerName, txID, metadataKey, 1)
}

// TransactionReverseMapKeyV — same versioning shape as
// AccountReverseMapKeyV.
//
//	[0x03][ledgerName padded 64B][t:][txID(8B)][version:4B BE][metadataKey]
func TransactionReverseMapKeyV(kb *dal.KeyBuilder, ledgerName string, txID uint64, metadataKey string, version uint32) []byte {
	return kb.Reset().
		PutByte(PrefixReverseMap).
		PutLedgerNameFixed(ledgerName).
		PutNamespace(NamespaceTransaction).
		PutUint64(txID).
		PutUint32(version).
		PutString(metadataKey).
		Build()
}

// AccountTxKey builds an account-to-transaction mapping key.
//
//	[prefix][ledgerName padded 64B][accountAddress\x00][txID(8B)]
func AccountTxKey(kb *dal.KeyBuilder, prefix byte, ledgerName string, account string, txID uint64) []byte {
	return kb.Reset().
		PutByte(prefix).
		PutLedgerNameFixed(ledgerName).
		PutStringNull(account).
		PutUint64(txID).
		Consume()
}

// AccountTxPrefix returns the prefix for scanning all transactions for an account.
//
//	[prefix][ledgerName padded 64B][accountAddress\x00]
func AccountTxPrefix(kb *dal.KeyBuilder, prefix byte, ledgerName string, account string) []byte {
	return kb.Reset().
		PutByte(prefix).
		PutLedgerNameFixed(ledgerName).
		PutStringNull(account).
		Snapshot()
}

// AccountByAssetKey builds an account-by-asset inverted-index key. Presence-only
// (nil value). Order-preserving so a prefix scan on (assetBase, precision)
// yields every account that has touched that exact asset cell.
//
//	[0x0C][ledgerName padded 64B][assetBase\x00][precision(1B)][account]
func AccountByAssetKey(kb *dal.KeyBuilder, ledgerName, assetBase string, precision uint8, account string) []byte {
	return kb.Reset().
		PutByte(PrefixAccountByAsset).
		PutLedgerNameFixed(ledgerName).
		PutStringNull(assetBase).
		PutByte(precision).
		PutString(account).
		Build()
}

// AccountByAssetPrefix returns the scan prefix matching every account that has
// touched (assetBase, precision).
//
//	[0x0C][ledgerName padded 64B][assetBase\x00][precision(1B)]
func AccountByAssetPrefix(kb *dal.KeyBuilder, ledgerName, assetBase string, precision uint8) []byte {
	return kb.Reset().
		PutByte(PrefixAccountByAsset).
		PutLedgerNameFixed(ledgerName).
		PutStringNull(assetBase).
		PutByte(precision).
		Snapshot()
}

// EntityExistsKey builds a full entity-ordered existence index key
// under version 1. Equivalent to EntityExistsKeyV(..., 1, ...).
func EntityExistsKey(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string, isNull bool, entityID []byte) []byte {
	return EntityExistsKeyV(kb, ledgerName, ns, metaKey, 1, isNull, entityID)
}

// EntityExistsKeyV builds a full entity-ordered existence index key.
// Forward-encoding version sits between the metadata key separator
// and the null flag (see MetadataIndexKeyV).
//
//	[0x02][ledgerName padded 64B][ns:][metadataKey\x00][version:4B BE][nullFlag][entityID]
func EntityExistsKeyV(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string, version uint32, isNull bool, entityID []byte) []byte {
	nullFlag := EntityExistsNonNull
	if isNull {
		nullFlag = EntityExistsNull
	}

	return kb.Reset().
		PutByte(PrefixEntityExists).
		PutLedgerNameFixed(ledgerName).
		PutNamespace(ns).
		PutStringNull(metaKey).
		PutUint32(version).
		PutByte(nullFlag).
		PutBytes(entityID).
		Consume()
}

// EntityExistsKeyPrefix returns the prefix covering both null and
// non-null entries for a given metadata key under version 1.
func EntityExistsKeyPrefix(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string) []byte {
	return EntityExistsKeyPrefixV(kb, ledgerName, ns, metaKey, 1)
}

// EntityExistsKeyPrefixV — version-aware variant.
//
//	[0x02][ledgerName padded 64B][ns:][metadataKey\x00][version:4B BE]
func EntityExistsKeyPrefixV(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string, version uint32) []byte {
	return kb.Reset().
		PutByte(PrefixEntityExists).
		PutLedgerNameFixed(ledgerName).
		PutNamespace(ns).
		PutStringNull(metaKey).
		PutUint32(version).
		Snapshot()
}

// EntityExistsNonNullPrefix returns the v=1 prefix for scanning non-null
// entities. Kept as a v=1 wrapper for callers not yet aware of versioning;
// version-aware sites must use EntityExistsNonNullPrefixV.
func EntityExistsNonNullPrefix(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string) []byte {
	return EntityExistsNonNullPrefixV(kb, ledgerName, ns, metaKey, 1)
}

// EntityExistsNonNullPrefixV returns the per-version prefix for scanning
// non-null entities. The 4-byte big-endian version sits between
// metaKey\x00 and the nullFlag — same layout as EntityExistsKeyV — so
// the prefix actually matches what the writer emits.
//
//	[0x02][ledgerName padded 64B][ns:][metadataKey\x00][version:4B BE][0x00]
func EntityExistsNonNullPrefixV(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string, version uint32) []byte {
	return kb.Reset().
		PutByte(PrefixEntityExists).
		PutLedgerNameFixed(ledgerName).
		PutNamespace(ns).
		PutStringNull(metaKey).
		PutUint32(version).
		PutByte(EntityExistsNonNull).
		Snapshot()
}

// EntityExistsNullPrefix returns the v=1 prefix for scanning null-valued
// entities. Kept as a v=1 wrapper; version-aware sites must use
// EntityExistsNullPrefixV.
func EntityExistsNullPrefix(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string) []byte {
	return EntityExistsNullPrefixV(kb, ledgerName, ns, metaKey, 1)
}

// EntityExistsNullPrefixV — version-aware variant of EntityExistsNullPrefix.
//
//	[0x02][ledgerName padded 64B][ns:][metadataKey\x00][version:4B BE][0x01]
func EntityExistsNullPrefixV(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string, version uint32) []byte {
	return kb.Reset().
		PutByte(PrefixEntityExists).
		PutLedgerNameFixed(ledgerName).
		PutNamespace(ns).
		PutStringNull(metaKey).
		PutUint32(version).
		PutByte(EntityExistsNull).
		Snapshot()
}

// MetadataIndexFieldPrefix returns the field-wide prefix that covers
// every version of a metadata key's forward index. Used by
// RemoveMetadataFieldType to DeleteRange across all v_n in one
// operation instead of per-version cleanup.
//
//	[0x01][ledgerName padded 64B][ns:][metadataKey\x00]
func MetadataIndexFieldPrefix(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string) []byte {
	return kb.Reset().
		PutByte(PrefixMetadataIndex).
		PutLedgerNameFixed(ledgerName).
		PutNamespace(ns).
		PutStringNull(metaKey).
		Snapshot()
}

// EntityExistsFieldPrefix returns the field-wide prefix that covers
// every version of a metadata key's eidx. Same role as
// MetadataIndexFieldPrefix on the eidx side.
//
//	[0x02][ledgerName padded 64B][ns:][metadataKey\x00]
func EntityExistsFieldPrefix(kb *dal.KeyBuilder, ledgerName string, ns, metaKey string) []byte {
	return kb.Reset().
		PutByte(PrefixEntityExists).
		PutLedgerNameFixed(ledgerName).
		PutNamespace(ns).
		PutStringNull(metaKey).
		Snapshot()
}

// TransactionReferenceKey builds a full key in the transaction reference index.
//
//	[0x07][ledgerName padded 64B][reference\x00][txID_BE(8B)]
func TransactionReferenceKey(kb *dal.KeyBuilder, ledgerName string, reference string, txID uint64) []byte {
	return kb.Reset().
		PutByte(PrefixTransactionReference).
		PutLedgerNameFixed(ledgerName).
		PutStringNull(reference).
		PutUint64(txID).
		Consume()
}

// TransactionReferencePrefix returns the prefix for scanning all txIDs with a given reference.
//
//	[0x07][ledgerName padded 64B][reference\x00]
func TransactionReferencePrefix(kb *dal.KeyBuilder, ledgerName string, reference string) []byte {
	return kb.Reset().
		PutByte(PrefixTransactionReference).
		PutLedgerNameFixed(ledgerName).
		PutStringNull(reference).
		Snapshot()
}

// TransactionTimestampKey builds a full key in the transaction timestamp index.
//
//	[0x08][ledgerName padded 64B][timestamp_BE(8B)][txID_BE(8B)]
func TransactionTimestampKey(kb *dal.KeyBuilder, ledgerName string, timestamp, txID uint64) []byte {
	return kb.Reset().
		PutByte(PrefixTransactionTimestamp).
		PutLedgerNameFixed(ledgerName).
		PutUint64(timestamp).
		PutUint64(txID).
		Consume()
}

// TransactionTimestampRangePrefix returns the ledger prefix for range scans in the timestamp index.
//
//	[0x08][ledgerName padded 64B]
func TransactionTimestampRangePrefix(kb *dal.KeyBuilder, ledgerName string) []byte {
	return kb.Reset().
		PutByte(PrefixTransactionTimestamp).
		PutLedgerNameFixed(ledgerName).
		Snapshot()
}

// TransactionInsertedAtKey builds a full key in the transaction inserted_at index.
//
//	[0x0B][ledgerName padded 64B][timestamp_BE(8B)][txID_BE(8B)]
func TransactionInsertedAtKey(kb *dal.KeyBuilder, ledgerName string, timestamp, txID uint64) []byte {
	return kb.Reset().
		PutByte(PrefixTransactionInsertedAt).
		PutLedgerNameFixed(ledgerName).
		PutUint64(timestamp).
		PutUint64(txID).
		Consume()
}

// TransactionInsertedAtRangePrefix returns the ledger prefix for range scans in the inserted_at index.
//
//	[0x0B][ledgerName padded 64B]
func TransactionInsertedAtRangePrefix(kb *dal.KeyBuilder, ledgerName string) []byte {
	return kb.Reset().
		PutByte(PrefixTransactionInsertedAt).
		PutLedgerNameFixed(ledgerName).
		Snapshot()
}

// LedgerLogKey builds a full key in the ledger logs index.
//
//	[0x09][ledgerName padded 64B][ledgerLogID_BE(8B)]
func LedgerLogKey(kb *dal.KeyBuilder, ledgerName string, logID uint64) []byte {
	return kb.Reset().
		PutByte(PrefixLedgerLogs).
		PutLedgerNameFixed(ledgerName).
		PutUint64(logID).
		Consume()
}

// LedgerLogPrefix returns the ledger prefix for range scans in the ledger logs index.
//
//	[0x09][ledgerName padded 64B]
func LedgerLogPrefix(kb *dal.KeyBuilder, ledgerName string) []byte {
	return kb.Reset().
		PutByte(PrefixLedgerLogs).
		PutLedgerNameFixed(ledgerName).
		Snapshot()
}

// LedgerLogDateKey builds a full key in the ledger log date index.
//
//	[0x0A][ledgerName padded 64B][timestamp_BE(8B)][logID_BE(8B)]
func LedgerLogDateKey(kb *dal.KeyBuilder, ledgerName string, timestamp, logID uint64) []byte {
	return kb.Reset().
		PutByte(PrefixLedgerLogDate).
		PutLedgerNameFixed(ledgerName).
		PutUint64(timestamp).
		PutUint64(logID).
		Consume()
}

// LedgerLogDateRangePrefix returns the ledger prefix for range scans in the log date index.
//
//	[0x0A][ledgerName padded 64B]
func LedgerLogDateRangePrefix(kb *dal.KeyBuilder, ledgerName string) []byte {
	return kb.Reset().
		PutByte(PrefixLedgerLogDate).
		PutLedgerNameFixed(ledgerName).
		Snapshot()
}

// ReverseMapPrefix returns the prefix for scanning reverse map entries
// within a namespace.
//
//	[0x03][ledgerName padded 64B][ns:]
func ReverseMapPrefix(kb *dal.KeyBuilder, ledgerName string, ns string) []byte {
	return kb.Reset().
		PutByte(PrefixReverseMap).
		PutLedgerNameFixed(ledgerName).
		PutNamespace(ns).
		Snapshot()
}

// BackfillKeyPrefix returns the prefix bytes for backfill keys.
//
//	[0xFE][0x03]
func BackfillKeyPrefix() []byte {
	return []byte{PrefixInternal, SubInternalBackfill}
}

// ProgressKey returns the full key for the progress entry.
//
//	[0xFE][0x01]
func ProgressKey() []byte {
	return []byte{PrefixInternal, SubInternalProgress}
}

// IndexVersionStateKey builds the per-(ledger, indexID) key under which a
// replica persists its forward-encoding version state: (current_version,
// pending_version, rewrite_progress). canonicalID is the canonical bytes
// of the IndexID (see indexes.Canonical).
//
//	[0xFE][0x04][ledgerName padded 64B][canonicalID]
func IndexVersionStateKey(kb *dal.KeyBuilder, ledgerName string, canonicalID string) []byte {
	return kb.Reset().
		PutByte(PrefixInternal).
		PutByte(SubInternalIndexVersion).
		PutLedgerNameFixed(ledgerName).
		PutString(canonicalID).
		Consume()
}

// IndexVersionStatePrefix returns the global prefix under which every
// per-index version state lives — used for boot-time scans and DeleteRange
// when a ledger is dropped.
//
//	[0xFE][0x04]
func IndexVersionStatePrefix() []byte {
	return []byte{PrefixInternal, SubInternalIndexVersion}
}

// AppliedProposalProgressKey returns the full key for the AppliedProposal
// sync progress entry.
//
//	[0xFE][0x02]
func AppliedProposalProgressKey() []byte {
	return []byte{PrefixInternal, SubInternalAppliedProposalProgress}
}

// AuditProgressKey returns the full key for the audit indexing cursor.
//
//	[0xFE][0x06]
func AuditProgressKey() []byte {
	return []byte{PrefixInternal, SubInternalAuditProgress}
}

// AuditIndexPrefix returns the global prefix for the whole audit index,
// used for DeleteRange on rebuild.
//
//	[0xFE][0x05]
func AuditIndexPrefix() []byte {
	return []byte{PrefixInternal, SubInternalAuditIndex}
}

// AuditIndexStringKey builds [0xFE][0x05][field][value\x00][seq BE8] for a
// string-valued field (ledger, caller_subject, order_type).
//
// The value is NUL-terminated and matched by prefix scan (AuditSeqsByString),
// so the encoding is unambiguous only while indexed values are themselves
// NUL-free — true today for order_type (fixed vocabulary), ledger (validated
// names) and caller.subject (auth subject). EN-1305, which wires the
// equality/range filter path over arbitrary caller subjects, MUST disambiguate
// before relying on it (an exact-length check len(key) == len(prefix)+8, or a
// length-prefixed string encoding); otherwise an "alice" lookup would also
// match a value indexed as "alice\x00evil".
func AuditIndexStringKey(kb *dal.KeyBuilder, field byte, value string, seq uint64) []byte {
	return kb.Reset().
		PutByte(PrefixInternal).
		PutByte(SubInternalAuditIndex).
		PutByte(field).
		PutStringNull(value).
		PutUint64(seq).
		Build()
}

// AuditIndexUint64Key builds [0xFE][0x05][field][value BE8][seq BE8] for a
// numeric range field (timestamp, proposal_id, log_seq).
func AuditIndexUint64Key(kb *dal.KeyBuilder, field byte, value, seq uint64) []byte {
	return kb.Reset().
		PutByte(PrefixInternal).
		PutByte(SubInternalAuditIndex).
		PutByte(field).
		PutUint64(value).
		PutUint64(seq).
		Build()
}

// AuditIndexByteKey builds [0xFE][0x05][field][value][seq BE8] for a
// single-byte field (outcome).
func AuditIndexByteKey(kb *dal.KeyBuilder, field, value byte, seq uint64) []byte {
	return kb.Reset().
		PutByte(PrefixInternal).
		PutByte(SubInternalAuditIndex).
		PutByte(field).
		PutByte(value).
		PutUint64(seq).
		Build()
}
