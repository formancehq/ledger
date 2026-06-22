package plan

import (
	"github.com/formancehq/ledger/v3/internal/domain"
)

// Needs describes the preload requirements for a command.
// All attribute types use map[K]struct{} and are resolved via attrs.*.Get.
type Needs struct {
	Ledgers           map[domain.LedgerKey]struct{}
	Boundaries        map[domain.LedgerKey]struct{}
	Volumes           map[domain.VolumeKey]struct{}
	IdempotencyKeys   map[domain.IdempotencyKey]struct{}
	References        map[domain.TransactionReferenceKey]struct{}
	Metadata          map[domain.MetadataKey]struct{}
	Transactions      map[domain.TransactionKey]struct{}
	SinkConfigs       map[domain.SinkConfigKey]struct{}
	NumscriptVersions map[domain.NumscriptVersionKey]struct{}
	NumscriptContents map[domain.NumscriptEntryKey]struct{}
	PreparedQueries   map[domain.PreparedQueryKey]struct{}
	LedgerMetadata    map[domain.LedgerMetadataKey]struct{}
}

// TotalKeys returns the total number of keys across all need types,
// including IdempotencyKeys. Used by admission metrics that account for
// every key the preload pipeline handles.
func (n *Needs) TotalKeys() int {
	return n.AttributeKeysCount() + len(n.IdempotencyKeys)
}

// AttributeKeysCount returns the total number of cache-attribute keys —
// i.e. every key that consults the in-memory cache at apply time.
// Idempotency keys live in the IdempotencyStore (not the cache), so
// they are excluded: a proposal with idempotency keys only does not
// need the cache-epoch revalidation that the slow path performs. The
// runner uses this count to gate runWithoutPreload, so idempotency-only
// proposals (maintenance, signing, chapter schedule) take the fast
// path and avoid spurious ErrStaleProposal on cluster-config resets.
func (n *Needs) AttributeKeysCount() int {
	return len(n.Ledgers) + len(n.Boundaries) + len(n.Volumes) +
		len(n.References) + len(n.Metadata) + len(n.Transactions) +
		len(n.SinkConfigs) + len(n.NumscriptVersions) +
		len(n.NumscriptContents) + len(n.PreparedQueries) +
		len(n.LedgerMetadata)
}

// Merge unions every key set from src into dst. Used by admission to roll
// per-order Needs into a single proposal-wide Needs while keeping the
// per-order slice available for coverage_bits computation.
func (n *Needs) Merge(src *Needs) {
	for k := range src.Ledgers {
		n.Ledgers[k] = struct{}{}
	}

	for k := range src.Boundaries {
		n.Boundaries[k] = struct{}{}
	}

	for k := range src.Volumes {
		n.Volumes[k] = struct{}{}
	}

	for k := range src.IdempotencyKeys {
		n.IdempotencyKeys[k] = struct{}{}
	}

	for k := range src.References {
		n.References[k] = struct{}{}
	}

	for k := range src.Metadata {
		n.Metadata[k] = struct{}{}
	}

	for k := range src.Transactions {
		n.Transactions[k] = struct{}{}
	}

	for k := range src.SinkConfigs {
		n.SinkConfigs[k] = struct{}{}
	}

	for k := range src.NumscriptVersions {
		n.NumscriptVersions[k] = struct{}{}
	}

	for k := range src.NumscriptContents {
		n.NumscriptContents[k] = struct{}{}
	}

	for k := range src.PreparedQueries {
		n.PreparedQueries[k] = struct{}{}
	}

	for k := range src.LedgerMetadata {
		n.LedgerMetadata[k] = struct{}{}
	}
}

// NewNeeds creates a Needs with all maps initialized.
func NewNeeds() *Needs {
	return &Needs{
		Ledgers:           make(map[domain.LedgerKey]struct{}),
		Boundaries:        make(map[domain.LedgerKey]struct{}),
		Volumes:           make(map[domain.VolumeKey]struct{}),
		IdempotencyKeys:   make(map[domain.IdempotencyKey]struct{}),
		References:        make(map[domain.TransactionReferenceKey]struct{}),
		Metadata:          make(map[domain.MetadataKey]struct{}),
		Transactions:      make(map[domain.TransactionKey]struct{}),
		SinkConfigs:       make(map[domain.SinkConfigKey]struct{}),
		NumscriptVersions: make(map[domain.NumscriptVersionKey]struct{}),
		NumscriptContents: make(map[domain.NumscriptEntryKey]struct{}),
		PreparedQueries:   make(map[domain.PreparedQueryKey]struct{}),
		LedgerMetadata:    make(map[domain.LedgerMetadataKey]struct{}),
	}
}
