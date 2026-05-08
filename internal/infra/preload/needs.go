package preload

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
)

// Needs describes the preload requirements for a command.
// All attribute types use map[K]struct{} and are resolved via attrs.*.Get.
type Needs struct {
	Ledgers           map[domain.LedgerKey]struct{}
	Boundaries        map[domain.LedgerKey]struct{}
	Volumes           map[domain.VolumeKey]struct{}
	TransientVolumes  map[domain.VolumeKey]struct{} // Transient accounts: zero-initialized preload, no Pebble lookup
	IdempotencyKeys   map[domain.IdempotencyKey]struct{}
	References        map[domain.TransactionReferenceKey]struct{}
	Metadata          map[domain.MetadataKey]struct{}
	Transactions      map[domain.TransactionKey]struct{}
	SinkConfigs       map[domain.SinkConfigKey]struct{}
	NumscriptVersions map[domain.NumscriptVersionKey]struct{}
	NumscriptContents map[domain.NumscriptEntryKey]struct{}
	PreparedQueries   map[domain.PreparedQueryKey]struct{}
}

// TotalKeys returns the total number of keys across all need types.
func (n *Needs) TotalKeys() int {
	return len(n.Ledgers) + len(n.Boundaries) + len(n.Volumes) +
		len(n.TransientVolumes) + len(n.IdempotencyKeys) + len(n.References) +
		len(n.Metadata) + len(n.Transactions) +
		len(n.SinkConfigs) + len(n.NumscriptVersions) +
		len(n.NumscriptContents) + len(n.PreparedQueries)
}

// NewNeeds creates a Needs with all maps initialized.
func NewNeeds() *Needs {
	return &Needs{
		Ledgers:           make(map[domain.LedgerKey]struct{}),
		Boundaries:        make(map[domain.LedgerKey]struct{}),
		Volumes:           make(map[domain.VolumeKey]struct{}),
		TransientVolumes:  make(map[domain.VolumeKey]struct{}),
		IdempotencyKeys:   make(map[domain.IdempotencyKey]struct{}),
		References:        make(map[domain.TransactionReferenceKey]struct{}),
		Metadata:          make(map[domain.MetadataKey]struct{}),
		Transactions:      make(map[domain.TransactionKey]struct{}),
		SinkConfigs:       make(map[domain.SinkConfigKey]struct{}),
		NumscriptVersions: make(map[domain.NumscriptVersionKey]struct{}),
		NumscriptContents: make(map[domain.NumscriptEntryKey]struct{}),
		PreparedQueries:   make(map[domain.PreparedQueryKey]struct{}),
	}
}
