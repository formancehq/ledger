package state

import (
	"slices"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// DerivedRegistry wraps DerivedKeyStores — one per attribute type — plus a
// pending reversion set. It is the transactional overlay used by WriteSet:
// writes go to the derived stores and are merged back into the parent
// StateRegistry on commit.
type DerivedRegistry struct {
	Volumes           *attributes.DerivedKeyStore[domain.VolumeKey, *raftcmdpb.VolumePair]
	AccountMetadata   *attributes.DerivedKeyStore[domain.MetadataKey, *commonpb.MetadataValue]
	Idempotency       *DerivedIdempotencyStore
	References        *attributes.DerivedKeyStore[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue]
	Ledgers           *attributes.DerivedKeyStore[domain.LedgerKey, *commonpb.LedgerInfo]
	Boundaries        *attributes.DerivedKeyStore[domain.LedgerKey, *raftcmdpb.LedgerBoundaries]
	SinkConfigs       *attributes.DerivedKeyStore[domain.SinkConfigKey, *commonpb.SinkConfig]
	NumscriptVersions *attributes.DerivedKeyStore[domain.NumscriptVersionKey, *commonpb.NumscriptVersionValue]
	Transactions      *attributes.DerivedKeyStore[domain.TransactionKey, *commonpb.TransactionState]
	NumscriptContents *attributes.DerivedKeyStore[domain.NumscriptEntryKey, *commonpb.NumscriptInfo]
	PreparedQueries   *attributes.DerivedKeyStore[domain.PreparedQueryKey, *commonpb.PreparedQuery]
	LedgerMetadata    *attributes.DerivedKeyStore[domain.LedgerMetadataKey, *commonpb.MetadataValue]
	Indexes           *attributes.DerivedKeyStore[domain.IndexKey, *commonpb.Index]

	// PendingReversions holds transaction keys marked as reverted in the
	// current proposal. These are flushed to the parent bitset on Merge.
	PendingReversions []domain.TransactionKey

	// parent is the parent StateRegistry, used to access the authoritative
	// reversion bitset. We store the registry pointer (not the map) so that
	// if RecoverState replaces Registry.Reversions after the DerivedRegistry
	// is created, GetReverted still reads from the current map.
	parent *StateRegistry
}

// NewDerivedRegistry creates a DerivedRegistry from a parent StateRegistry.
// Each DerivedKeyStore reads from the parent KeyStore (extracted via .KeyStore())
// and buffers writes locally.
func NewDerivedRegistry(reg *StateRegistry) *DerivedRegistry {
	return &DerivedRegistry{
		Volumes:           attributes.NewDerivedKeyStore(reg.Volumes.KeyStore()),
		Boundaries:        attributes.NewDerivedKeyStore(reg.Boundaries.KeyStore()),
		AccountMetadata:   attributes.NewDerivedKeyStore(reg.AccountMetadata.KeyStore()),
		Idempotency:       NewDerivedIdempotencyStore(reg.Idempotency),
		References:        attributes.NewDerivedKeyStore(reg.References.KeyStore()),
		Ledgers:           attributes.NewDerivedKeyStore(reg.Ledgers.KeyStore()),
		SinkConfigs:       attributes.NewDerivedKeyStore(reg.SinkConfigs.KeyStore()),
		NumscriptVersions: attributes.NewDerivedKeyStore(reg.NumscriptVersions.KeyStore()),
		Transactions:      attributes.NewDerivedKeyStore(reg.Transactions.KeyStore()),
		NumscriptContents: attributes.NewDerivedKeyStore(reg.NumscriptContents.KeyStore()),
		PreparedQueries:   attributes.NewDerivedKeyStore(reg.PreparedQueries.KeyStore()),
		LedgerMetadata:    attributes.NewDerivedKeyStore(reg.LedgerMetadata.KeyStore()),
		Indexes:           attributes.NewDerivedKeyStore(reg.Indexes.KeyStore()),
		parent:            reg,
	}
}

// Reset clears all derived stores for reuse without reallocating.
func (d *DerivedRegistry) Reset() {
	d.Volumes.Reset()
	d.AccountMetadata.Reset()
	d.Idempotency.Reset()
	d.References.Reset()
	d.Ledgers.Reset()
	d.Boundaries.Reset()
	d.SinkConfigs.Reset()
	d.NumscriptVersions.Reset()
	d.Transactions.Reset()
	d.NumscriptContents.Reset()
	d.PreparedQueries.Reset()
	d.LedgerMetadata.Reset()
	d.Indexes.Reset()
	d.PendingReversions = d.PendingReversions[:0]
}

// GetReverted checks pending reversions first, then the parent bitset.
func (d *DerivedRegistry) GetReverted(key domain.TransactionKey) bool {
	// Check pending reversions in this proposal
	if slices.Contains(d.PendingReversions, key) {
		return true
	}
	// Check the authoritative bitset
	bs, ok := d.parent.Reversions[key.LedgerName]
	if !ok {
		return false
	}

	return bs.Test(key.ID)
}

// PutReverted adds a transaction key to the pending reversions.
func (d *DerivedRegistry) PutReverted(key domain.TransactionKey) {
	d.PendingReversions = append(d.PendingReversions, key)
}
