package preload

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// Needs describes the preload requirements for a command.
// Standard attribute types use map[K]struct{} and are resolved via attrs.*.ComputeValue.
// Custom types (SinkConfigs, NumscriptVersions, NumscriptEntries) use map[K]func()(T, error)
// where the caller supplies the load function.
type Needs struct {
	Ledgers         map[domain.LedgerKey]struct{}
	Boundaries      map[domain.LedgerKey]struct{}
	Volumes         map[domain.VolumeKey]struct{}
	IdempotencyKeys map[domain.IdempotencyKey]struct{}
	References      map[domain.TransactionReferenceKey]struct{}
	Metadata        map[domain.MetadataKey]struct{}
	Transactions    map[domain.TransactionKey]struct{}

	// Custom loaders — value is the load function. Nil map = skip.
	SinkConfigs       map[domain.SinkConfigKey]func() (*commonpb.SinkConfig, error)
	NumscriptVersions map[domain.NumscriptVersionKey]func() (string, error)
	NumscriptEntries  map[domain.NumscriptEntryKey]func() (bool, error)
	NumscriptParsed   map[domain.NumscriptContentKey]func() (string, error)
}

// NewNeeds creates a Needs with all maps initialized.
func NewNeeds() *Needs {
	return &Needs{
		Ledgers:         make(map[domain.LedgerKey]struct{}),
		Boundaries:      make(map[domain.LedgerKey]struct{}),
		Volumes:         make(map[domain.VolumeKey]struct{}),
		IdempotencyKeys: make(map[domain.IdempotencyKey]struct{}),
		References:      make(map[domain.TransactionReferenceKey]struct{}),
		Metadata:        make(map[domain.MetadataKey]struct{}),
		Transactions:    make(map[domain.TransactionKey]struct{}),
	}
}
