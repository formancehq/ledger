package state

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
)

// StateRegistry groups the 8 KeyStores, Cache, and Attributes that hold the
// Machine's volatile in-memory state. Extracting them into a single struct
// reduces Machine's field count and gives a clear boundary around the "what
// data lives in memory" concern.
type StateRegistry struct {
	Cache           *cache.Cache
	Attrs           *attributes.Attributes
	Volumes         *attributes.KeyStore[domain.VolumeKey, *raftcmdpb.VolumePair]
	AccountMetadata *attributes.KeyStore[domain.MetadataKey, *commonpb.MetadataValue]
	Reversions      *attributes.KeyStore[domain.TransactionKey, bool]
	IdempotencyKeys *attributes.KeyStore[domain.IdempotencyKey, *commonpb.IdempotencyKeyValue]
	References      *attributes.KeyStore[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue]
	Ledgers         *attributes.KeyStore[domain.LedgerKey, *commonpb.LedgerInfo]
	Boundaries      *attributes.KeyStore[domain.LedgerKey, *raftcmdpb.LedgerBoundaries]
	SinkConfigs     *attributes.KeyStore[domain.SinkConfigKey, *commonpb.SinkConfig]
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
		Reversions: attributes.NewKeyStore[domain.TransactionKey, bool](
			attributes.DefaultSeeds,
			c.Reversions,
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
	}
}
