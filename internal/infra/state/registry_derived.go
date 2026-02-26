package state

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
)

// DerivedRegistry wraps DerivedKeyStores — one per attribute type — plus a
// pending reversion set. It is the transactional overlay used by Buffered:
// writes go to the derived stores and are merged back into the parent
// StateRegistry on commit.
type DerivedRegistry struct {
	Volumes         *attributes.DerivedKeyStore[domain.VolumeKey, *raftcmdpb.VolumePair]
	AccountMetadata *attributes.DerivedKeyStore[domain.MetadataKey, *commonpb.MetadataValue]
	IdempotencyKeys *attributes.DerivedKeyStore[domain.IdempotencyKey, *commonpb.IdempotencyKeyValue]
	References      *attributes.DerivedKeyStore[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue]
	Ledgers         *attributes.DerivedKeyStore[domain.LedgerKey, *commonpb.LedgerInfo]
	Boundaries      *attributes.DerivedKeyStore[domain.LedgerKey, *raftcmdpb.LedgerBoundaries]
	SinkConfigs     *attributes.DerivedKeyStore[domain.SinkConfigKey, *commonpb.SinkConfig]

	// PendingReversions holds transaction keys marked as reverted in the
	// current proposal. These are flushed to the parent bitset on Merge.
	PendingReversions []domain.TransactionKey

	// parent is the authoritative reversion bitset map (from StateRegistry).
	parentReversions map[string]*domain.ReversionBitset
}

// NewDerivedRegistry creates a DerivedRegistry from a parent StateRegistry.
// Each DerivedKeyStore reads from the parent KeyStore and buffers writes locally.
func NewDerivedRegistry(reg *StateRegistry) *DerivedRegistry {
	return &DerivedRegistry{
		Volumes:          attributes.NewDerivedKeyStore(reg.Volumes, (*raftcmdpb.VolumePair).CloneVT),
		AccountMetadata:  attributes.NewDerivedKeyStore(reg.AccountMetadata, (*commonpb.MetadataValue).CloneVT),
		IdempotencyKeys:  attributes.NewDerivedKeyStore(reg.IdempotencyKeys, (*commonpb.IdempotencyKeyValue).CloneVT),
		References:       attributes.NewDerivedKeyStore(reg.References, (*commonpb.TransactionReferenceValue).CloneVT),
		Ledgers:          attributes.NewDerivedKeyStore(reg.Ledgers, (*commonpb.LedgerInfo).CloneVT),
		Boundaries:       attributes.NewDerivedKeyStore(reg.Boundaries, (*raftcmdpb.LedgerBoundaries).CloneVT),
		SinkConfigs:      attributes.NewDerivedKeyStore(reg.SinkConfigs, (*commonpb.SinkConfig).CloneVT),
		parentReversions: reg.Reversions,
	}
}

// GetReverted checks pending reversions first, then the parent bitset.
func (d *DerivedRegistry) GetReverted(key domain.TransactionKey) bool {
	// Check pending reversions in this proposal
	for _, k := range d.PendingReversions {
		if k == key {
			return true
		}
	}
	// Check the authoritative bitset
	bs, ok := d.parentReversions[key.Ledger]
	if !ok {
		return false
	}
	return bs.IsReverted(key.ID)
}

// PutReverted adds a transaction key to the pending reversions.
func (d *DerivedRegistry) PutReverted(key domain.TransactionKey) {
	d.PendingReversions = append(d.PendingReversions, key)
}
