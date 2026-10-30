package query

import (
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// CompileOptions holds the common inputs for forward and reverse compilation.
// All resources are borrowed: callers retain ownership of snapshots, leases,
// and cleanup. Copying options does not copy maps or protobuf messages, or make
// the mutable KeyBuilder and Profile safe for concurrent use.
type CompileOptions struct {
	// IndexReader is the snapshot used by index iterators. IndexVersionFor must
	// resolve versions through this same snapshot, at Pin, so a concurrent
	// version switch cannot select a keyspace absent from the snapshot.
	IndexReader     dal.PebbleReader
	IndexVersionFor readstore.IndexVersionResolver

	// MainReader is the main-store snapshot used by primary-store leaves and
	// enrichment. Pin is that snapshot's native applied sequence, used for
	// version/event resolution and main-horizon trimming. For indexed filters,
	// callers certify IndexReader against MainReader's fixed Raft applied index
	// through AlignedIndexSnapshot. This certificate is separate from Pin;
	// alignment and trimming remain caller responsibilities.
	MainReader dal.PebbleReader
	Pin        uint64

	KeyBuilder *dal.KeyBuilder
	Target     commonpb.QueryTarget
	LedgerName string
	Parameters map[string]*commonpb.ParameterValue

	// LedgerInfo, Schema, and IndexRegistry come from MainReader's snapshot.
	// Schema validates metadata condition types; nil rejects metadata filters
	// with ErrIndexNotFound. IndexRegistry gates index-bound conditions on a
	// registered, ready index; nil rejects those conditions with ErrIndexNotFound.
	LedgerInfo    *commonpb.LedgerInfo
	Schema        map[string]*commonpb.MetadataFieldSchema
	IndexRegistry indexes.Lookup

	// Profile, when non-nil, tracks iterators and receives their root stats.
	Profile *QueryProfile
}

// newCompileCtx gives each compilation its own recursive depth state without
// mutating the supplied options or transferring ownership of their resources.
func newCompileCtx(options CompileOptions) *compileCtx {
	indexVersionFor := options.IndexVersionFor
	if indexVersionFor == nil {
		// Preserve the legacy v1 fallback for ad-hoc callers and tests.
		// Production supplies a resolver bound to IndexReader and Pin.
		indexVersionFor = func(string) (readstore.ResolvedIndexVersion, bool, error) {
			return readstore.ResolvedIndexVersion{Version: 1}, true, nil
		}
	}

	return &compileCtx{
		kb:              options.KeyBuilder,
		pebbleReader:    options.MainReader,
		indexReader:     options.IndexReader,
		target:          options.Target,
		ledgerName:      options.LedgerName,
		params:          options.Parameters,
		schema:          options.Schema,
		info:            options.LedgerInfo,
		indexRegistry:   options.IndexRegistry,
		indexVersionFor: indexVersionFor,
		pin:             options.Pin,
		profile:         options.Profile,
	}
}
