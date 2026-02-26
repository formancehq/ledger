package state

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
)

// DerivedRegistry wraps 8 DerivedKeyStores — one per attribute type. It is the
// transactional overlay used by Buffered: writes go to the derived stores and
// are merged back into the parent StateRegistry on commit.
type DerivedRegistry struct {
	Volumes         *attributes.DerivedKeyStore[domain.VolumeKey, *raftcmdpb.VolumePair]
	AccountMetadata *attributes.DerivedKeyStore[domain.MetadataKey, *commonpb.MetadataValue]
	Reversions      *attributes.DerivedKeyStore[domain.TransactionKey, bool]
	IdempotencyKeys *attributes.DerivedKeyStore[domain.IdempotencyKey, *commonpb.IdempotencyKeyValue]
	References      *attributes.DerivedKeyStore[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue]
	Ledgers         *attributes.DerivedKeyStore[domain.LedgerKey, *commonpb.LedgerInfo]
	Boundaries      *attributes.DerivedKeyStore[domain.LedgerKey, *raftcmdpb.LedgerBoundaries]
	SinkConfigs     *attributes.DerivedKeyStore[domain.SinkConfigKey, *commonpb.SinkConfig]
}

// NewDerivedRegistry creates a DerivedRegistry from a parent StateRegistry.
// Each DerivedKeyStore reads from the parent KeyStore and buffers writes locally.
func NewDerivedRegistry(reg *StateRegistry) *DerivedRegistry {
	return &DerivedRegistry{
		Volumes:         attributes.NewDerivedKeyStore(reg.Volumes, (*raftcmdpb.VolumePair).CloneVT),
		AccountMetadata: attributes.NewDerivedKeyStore(reg.AccountMetadata, (*commonpb.MetadataValue).CloneVT),
		Reversions:      attributes.NewDerivedKeyStore(reg.Reversions, nil), // bool is a value type
		IdempotencyKeys: attributes.NewDerivedKeyStore(reg.IdempotencyKeys, (*commonpb.IdempotencyKeyValue).CloneVT),
		References:      attributes.NewDerivedKeyStore(reg.References, (*commonpb.TransactionReferenceValue).CloneVT),
		Ledgers:         attributes.NewDerivedKeyStore(reg.Ledgers, (*commonpb.LedgerInfo).CloneVT),
		Boundaries:      attributes.NewDerivedKeyStore(reg.Boundaries, (*raftcmdpb.LedgerBoundaries).CloneVT),
		SinkConfigs:     attributes.NewDerivedKeyStore(reg.SinkConfigs, (*commonpb.SinkConfig).CloneVT),
	}
}
