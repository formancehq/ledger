package state

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// StateRegistry groups the CacheAwareEntries, Cache, Attributes, and
// ReversionBitsets that hold the Machine's volatile in-memory state.
// Extracting them into a single struct reduces Machine's field count and gives
// a clear boundary around the "what data lives in memory" concern.
//
// Each attribute type is wrapped in a CacheAwareEntry that bundles the
// in-memory KeyStore, its Pebble Attribute (0xF1), and the 0xFF cache type
// byte. This makes it structurally impossible to write to the in-memory cache
// without also writing to the 0xFF cache zone — preventing cache divergence
// bugs after node restart.
type StateRegistry struct {
	Cache             *cache.Cache
	Attrs             *attributes.Attributes
	Volumes           *CacheAwareEntry[domain.VolumeKey, *raftcmdpb.VolumePair]
	AccountMetadata   *CacheAwareEntry[domain.MetadataKey, *commonpb.MetadataValue]
	Idempotency       *IdempotencyStore
	References        *CacheAwareEntry[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue]
	Ledgers           *CacheAwareEntry[domain.LedgerKey, *commonpb.LedgerInfo]
	Boundaries        *CacheAwareEntry[domain.LedgerKey, *raftcmdpb.LedgerBoundaries]
	SinkConfigs       *CacheAwareEntry[domain.SinkConfigKey, *commonpb.SinkConfig]
	NumscriptVersions *CacheAwareEntry[domain.NumscriptVersionKey, *commonpb.NumscriptVersionValue]
	Transactions      *CacheAwareEntry[domain.TransactionKey, *commonpb.TransactionState]
	NumscriptContents *CacheAwareEntry[domain.NumscriptEntryKey, *commonpb.NumscriptInfo]
	PreparedQueries   *CacheAwareEntry[domain.PreparedQueryKey, *commonpb.PreparedQuery]
	LedgerMetadata    *CacheAwareEntry[domain.LedgerMetadataKey, *commonpb.MetadataValue]
	Indexes           *CacheAwareEntry[domain.IndexKey, *commonpb.Index]

	// Reversions uses a compact bitset per ledger instead of a KeyStore.
	// Bit N being set means transaction N in that ledger has been reverted.
	// This is always authoritative (no cache generations, no preload needed).
	// Keyed by ledger name (matches the canonical-key change).
	Reversions map[string]*bitset.Bitset

	// BackupJobs is the FSM-owned view of in-flight backups, keyed by
	// canonical destination. See backup_jobs.go for the rationale: the
	// gRPC handler used to take an in-memory mutex and bypass Raft; we
	// move the state inside the FSM so cross-node mutual exclusion is
	// inherited from Raft's apply determinism, no separate lease primitive
	// or wall-clock decisions involved.
	BackupJobs *BackupJobsState
}

// NewStateRegistry creates a StateRegistry with all CacheAwareEntries backed
// by the given cache. idempotencyTTLMicros is the TTL in HLC microseconds (0 = no expiration).
func NewStateRegistry(c *cache.Cache, attrs *attributes.Attributes, idempotencyTTLMicros uint64) *StateRegistry {
	return &StateRegistry{
		Cache: c,
		Attrs: attrs,
		Volumes: NewCacheAwareEntry(
			attributes.NewKeyStore[domain.VolumeKey, *raftcmdpb.VolumePair](c.Volumes),
			attrs.Volume,
			dal.SubAttrVolume,
		),
		AccountMetadata: NewCacheAwareEntry(
			attributes.NewKeyStore[domain.MetadataKey, *commonpb.MetadataValue](c.AccountMetadata),
			attrs.Metadata,
			dal.SubAttrMetadata,
		),
		Idempotency: NewIdempotencyStore(idempotencyTTLMicros),
		References: NewCacheAwareEntry(
			attributes.NewKeyStore[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue](c.References),
			attrs.References,
			dal.SubAttrReference,
		),
		Ledgers: NewCacheAwareEntry(
			attributes.NewKeyStore[domain.LedgerKey, *commonpb.LedgerInfo](c.Ledgers),
			attrs.Ledger,
			dal.SubAttrLedger,
		),
		Boundaries: NewCacheAwareEntry(
			attributes.NewKeyStore[domain.LedgerKey, *raftcmdpb.LedgerBoundaries](c.Boundaries),
			attrs.Boundary,
			dal.SubAttrBoundary,
		),
		SinkConfigs: NewCacheAwareEntry(
			attributes.NewKeyStore[domain.SinkConfigKey, *commonpb.SinkConfig](c.SinkConfigs),
			attrs.SinkConfig,
			dal.SubAttrSinkConfig,
		),
		NumscriptVersions: NewCacheAwareEntry(
			attributes.NewKeyStore[domain.NumscriptVersionKey, *commonpb.NumscriptVersionValue](c.NumscriptVersions),
			attrs.NumscriptVersion,
			dal.SubAttrNumscriptVersion,
		),
		Transactions: NewCacheAwareEntry(
			attributes.NewKeyStore[domain.TransactionKey, *commonpb.TransactionState](c.Transactions),
			attrs.Transaction,
			dal.SubAttrTransaction,
		),
		NumscriptContents: NewCacheAwareEntry(
			attributes.NewKeyStore[domain.NumscriptEntryKey, *commonpb.NumscriptInfo](c.NumscriptContents),
			attrs.NumscriptContent,
			dal.SubAttrNumscriptContent,
		),
		PreparedQueries: NewCacheAwareEntry(
			attributes.NewKeyStore[domain.PreparedQueryKey, *commonpb.PreparedQuery](c.PreparedQueries),
			attrs.PreparedQuery,
			dal.SubAttrPreparedQuery,
		),
		LedgerMetadata: NewCacheAwareEntry(
			attributes.NewKeyStore[domain.LedgerMetadataKey, *commonpb.MetadataValue](c.LedgerMetadata),
			attrs.LedgerMetadata,
			dal.SubAttrLedgerMetadata,
		),
		Indexes: NewCacheAwareEntry(
			attributes.NewKeyStore[domain.IndexKey, *commonpb.Index](c.Indexes),
			attrs.Index,
			dal.SubAttrIndex,
		),
		Reversions: make(map[string]*bitset.Bitset),
		BackupJobs: NewBackupJobsState(),
	}
}

// GetReverted returns whether a transaction has been reverted.
func (r *StateRegistry) GetReverted(key domain.TransactionKey) bool {
	bs, ok := r.Reversions[key.LedgerName]
	if !ok {
		return false
	}

	return bs.Test(key.ID)
}

// SetReverted marks a transaction as reverted in the bitset.
// Returns the word index that was modified.
func (r *StateRegistry) SetReverted(key domain.TransactionKey) uint64 {
	bs, ok := r.Reversions[key.LedgerName]
	if !ok {
		bs = bitset.New(key.ID)
		r.Reversions[key.LedgerName] = bs
	}

	return bs.Set(key.ID)
}

// ResetReversions clears all reversion bitsets (used during snapshot restore).
func (r *StateRegistry) ResetReversions() {
	r.Reversions = make(map[string]*bitset.Bitset)
}
