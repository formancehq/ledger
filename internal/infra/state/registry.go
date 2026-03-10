package state

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// StateRegistry groups the KeyStores, Cache, Attributes, and ReversionBitsets
// that hold the Machine's volatile in-memory state. Extracting them into a
// single struct reduces Machine's field count and gives a clear boundary around
// the "what data lives in memory" concern.
type StateRegistry struct {
	Cache             *cache.Cache
	Attrs             *attributes.Attributes
	Volumes           *attributes.KeyStore[domain.VolumeKey, *raftcmdpb.VolumePair]
	AccountMetadata   *attributes.KeyStore[domain.MetadataKey, *commonpb.MetadataValue]
	IdempotencyKeys   *attributes.KeyStore[domain.IdempotencyKey, *commonpb.IdempotencyKeyValue]
	References        *attributes.KeyStore[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue]
	Ledgers           *attributes.KeyStore[domain.LedgerKey, *commonpb.LedgerInfo]
	Boundaries        *attributes.KeyStore[domain.LedgerKey, *raftcmdpb.LedgerBoundaries]
	SinkConfigs       *attributes.KeyStore[domain.SinkConfigKey, *commonpb.SinkConfig]
	NumscriptVersions *attributes.KeyStore[domain.NumscriptVersionKey, string]
	NumscriptEntries  *attributes.KeyStore[domain.NumscriptEntryKey, bool]
	Transactions      *attributes.KeyStore[domain.TransactionKey, *commonpb.TransactionState]

	// Reversions uses a compact bitset per ledger instead of a KeyStore.
	// Bit N being set means transaction N in that ledger has been reverted.
	// This is always authoritative (no cache generations, no preload needed).
	Reversions map[string]*domain.ReversionBitset
}

// NewStateRegistry creates a StateRegistry with all KeyStores backed by the
// given cache.
func NewStateRegistry(c *cache.Cache, attrs *attributes.Attributes) *StateRegistry {
	return &StateRegistry{
		Cache: c,
		Attrs: attrs,
		Volumes: attributes.NewKeyStore[domain.VolumeKey, *raftcmdpb.VolumePair](
			attributes.DefaultSeeds,
			c.Volumes,
		),
		AccountMetadata: attributes.NewKeyStore[domain.MetadataKey, *commonpb.MetadataValue](
			attributes.DefaultSeeds,
			c.AccountMetadata,
		),
		IdempotencyKeys: attributes.NewKeyStore[domain.IdempotencyKey, *commonpb.IdempotencyKeyValue](
			attributes.DefaultSeeds,
			c.IdempotencyKeys,
		),
		References: attributes.NewKeyStore[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue](
			attributes.DefaultSeeds,
			c.References,
		),
		Ledgers: attributes.NewKeyStore[domain.LedgerKey, *commonpb.LedgerInfo](
			attributes.DefaultSeeds,
			c.Ledgers,
		),
		Boundaries: attributes.NewKeyStore[domain.LedgerKey, *raftcmdpb.LedgerBoundaries](
			attributes.DefaultSeeds,
			c.Boundaries,
		),
		SinkConfigs: attributes.NewKeyStore[domain.SinkConfigKey, *commonpb.SinkConfig](
			attributes.DefaultSeeds,
			c.SinkConfigs,
		),
		NumscriptVersions: attributes.NewKeyStore[domain.NumscriptVersionKey, string](
			attributes.DefaultSeeds,
			c.NumscriptVersions,
		),
		NumscriptEntries: attributes.NewKeyStore[domain.NumscriptEntryKey, bool](
			attributes.DefaultSeeds,
			c.NumscriptEntries,
		),
		Transactions: attributes.NewKeyStore[domain.TransactionKey, *commonpb.TransactionState](
			attributes.DefaultSeeds,
			c.Transactions,
		),
		Reversions: make(map[string]*domain.ReversionBitset),
	}
}

// GetReverted returns whether a transaction has been reverted.
func (r *StateRegistry) GetReverted(key domain.TransactionKey) bool {
	bs, ok := r.Reversions[key.Ledger]
	if !ok {
		return false
	}

	return bs.IsReverted(key.ID)
}

// SetReverted marks a transaction as reverted in the bitset.
func (r *StateRegistry) SetReverted(key domain.TransactionKey) {
	bs, ok := r.Reversions[key.Ledger]
	if !ok {
		bs = domain.NewReversionBitset(key.ID)
		r.Reversions[key.Ledger] = bs
	}

	bs.SetReverted(key.ID)
}

// ResetReversions clears all reversion bitsets (used during snapshot restore).
func (r *StateRegistry) ResetReversions() {
	r.Reversions = make(map[string]*domain.ReversionBitset)
}
