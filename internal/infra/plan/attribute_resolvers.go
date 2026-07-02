package plan

import (
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/preload"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// attrResolver is the non-generic view over one attribute type's
// resolve pipeline. Each attribute cache carries a different generic
// value type T, so the concrete implementation is
// protoAttrResolver[T]; the interface exists so the builder can hold
// them uniformly in a map keyed by attrCode.
type attrResolver interface {
	// Resolve walks the keys set through resolveAttributePreload for
	// this attribute's cache/loader/store bindings. Returns the batch
	// of AttributePlan entries and the tracker keys the caller must
	// associate with Loader() for the CleanupToken.
	Resolve(
		keys map[string]struct{},
		nextIndex, boundary, cacheEpoch uint64,
		store dal.PebbleGetter,
		logger logging.Logger,
	) (*resolveResult, error)

	// Loader returns the AttributeLoader tied to this resolver so the
	// builder can register it in the CleanupToken. Non-generic so the
	// registration is uniform across attribute types.
	Loader() preload.LoaderOps
}

// protoAttrResolver binds one attribute cache/loader/store triple for
// the resolve pipeline. One instance per dal.SubAttrX code lives in
// Builder.resolvers, populated at NewBuilder.
//
// bloom is a closure rather than a captured pointer: bloom.FilterSet is
// swappable across the ledger lifetime and the Builder.bloomFilter
// accessor takes a fresh snapshot per call to keep readiness and the
// filter pointer coherent (see #317). Storing a stale pointer here
// would defeat that.
type protoAttrResolver[T interface {
	MarshalVT() ([]byte, error)
}] struct {
	attrCode byte
	typeName string
	cache    *cache.AttributeCache[T]
	loader   *preload.AttributeLoader[T]
	getValue func(reader dal.PebbleGetter, canonicalKey []byte) (T, error)
	bloom    func() *bloom.Filter
}

func (r *protoAttrResolver[T]) Resolve(
	keys map[string]struct{},
	nextIndex, boundary, cacheEpoch uint64,
	store dal.PebbleGetter,
	logger logging.Logger,
) (*resolveResult, error) {
	return resolveAttributePreload[T](
		keys, nextIndex, boundary, cacheEpoch,
		r.cache, r.loader, r.getValue, store,
		r.attrCode, nil, r.bloom(), logger, r.typeName,
	)
}

func (r *protoAttrResolver[T]) Loader() preload.LoaderOps { return r.loader }

// buildAttrResolvers returns the full registration set keyed by
// dal.SubAttrX for every attribute cache. Adding a new attribute
// cache means adding one entry here — no changes needed in Needs,
// admission call sites, coverage_bits, or the builder loop.
func buildAttrResolvers(
	c *cache.Cache,
	attrs *attributes.Attributes,
	loaders *preload.Loaders,
	bloomLookup func(sub byte) *bloom.Filter,
) map[byte]attrResolver {
	// bloomLookup takes a snapshot per call (see Builder.bloomFilter);
	// wrap it in per-attrCode closures so each resolver captures its own
	// attribute code without threading it through the Resolve signature.
	filter := func(sub byte) func() *bloom.Filter {
		return func() *bloom.Filter { return bloomLookup(sub) }
	}

	return map[byte]attrResolver{
		dal.SubAttrLedger: &protoAttrResolver[*commonpb.LedgerInfo]{
			attrCode: dal.SubAttrLedger,
			typeName: "ledgers",
			cache:    c.Ledgers,
			loader:   loaders.Ledgers,
			getValue: attrs.Ledger.Get,
			bloom:    filter(dal.SubAttrLedger),
		},
		dal.SubAttrBoundary: &protoAttrResolver[*raftcmdpb.LedgerBoundaries]{
			attrCode: dal.SubAttrBoundary,
			typeName: "boundaries",
			cache:    c.Boundaries,
			loader:   loaders.Boundaries,
			getValue: attrs.Boundary.Get,
			bloom:    filter(dal.SubAttrBoundary),
		},
		dal.SubAttrVolume: &protoAttrResolver[*raftcmdpb.VolumePair]{
			attrCode: dal.SubAttrVolume,
			typeName: "volumes",
			cache:    c.Volumes,
			loader:   loaders.Volumes,
			getValue: attrs.Volume.Get,
			bloom:    filter(dal.SubAttrVolume),
		},
		dal.SubAttrReference: &protoAttrResolver[*commonpb.TransactionReferenceValue]{
			attrCode: dal.SubAttrReference,
			typeName: "references",
			cache:    c.References,
			loader:   loaders.References,
			getValue: attrs.References.Get,
			bloom:    filter(dal.SubAttrReference),
		},
		dal.SubAttrSinkConfig: &protoAttrResolver[*commonpb.SinkConfig]{
			attrCode: dal.SubAttrSinkConfig,
			typeName: "sink_configs",
			cache:    c.SinkConfigs,
			loader:   loaders.SinkConfigs,
			getValue: attrs.SinkConfig.Get,
			bloom:    filter(dal.SubAttrSinkConfig),
		},
		dal.SubAttrNumscriptVersion: &protoAttrResolver[*commonpb.NumscriptVersionValue]{
			attrCode: dal.SubAttrNumscriptVersion,
			typeName: "numscript_versions",
			cache:    c.NumscriptVersions,
			loader:   loaders.NumscriptVersions,
			getValue: attrs.NumscriptVersion.Get,
			bloom:    filter(dal.SubAttrNumscriptVersion),
		},
		dal.SubAttrNumscriptContent: &protoAttrResolver[*commonpb.NumscriptInfo]{
			attrCode: dal.SubAttrNumscriptContent,
			typeName: "numscript_contents",
			cache:    c.NumscriptContents,
			loader:   loaders.NumscriptContents,
			getValue: attrs.NumscriptContent.Get,
			bloom:    filter(dal.SubAttrNumscriptContent),
		},
		dal.SubAttrTransaction: &protoAttrResolver[*commonpb.TransactionState]{
			attrCode: dal.SubAttrTransaction,
			typeName: "transactions",
			cache:    c.Transactions,
			loader:   loaders.Transactions,
			getValue: attrs.Transaction.Get,
			bloom:    filter(dal.SubAttrTransaction),
		},
		dal.SubAttrMetadata: &protoAttrResolver[*commonpb.MetadataValue]{
			attrCode: dal.SubAttrMetadata,
			typeName: "metadata",
			cache:    c.AccountMetadata,
			loader:   loaders.AccountMetadata,
			getValue: attrs.Metadata.Get,
			bloom:    filter(dal.SubAttrMetadata),
		},
		dal.SubAttrPreparedQuery: &protoAttrResolver[*commonpb.PreparedQuery]{
			attrCode: dal.SubAttrPreparedQuery,
			typeName: "prepared_queries",
			cache:    c.PreparedQueries,
			loader:   loaders.PreparedQueries,
			getValue: attrs.PreparedQuery.Get,
			bloom:    filter(dal.SubAttrPreparedQuery),
		},
		dal.SubAttrLedgerMetadata: &protoAttrResolver[*commonpb.MetadataValue]{
			attrCode: dal.SubAttrLedgerMetadata,
			typeName: "ledger_metadata",
			cache:    c.LedgerMetadata,
			loader:   loaders.LedgerMetadata,
			getValue: attrs.LedgerMetadata.Get,
			bloom:    filter(dal.SubAttrLedgerMetadata),
		},
		dal.SubAttrIndex: &protoAttrResolver[*commonpb.Index]{
			attrCode: dal.SubAttrIndex,
			typeName: "indexes",
			cache:    c.Indexes,
			loader:   loaders.Indexes,
			getValue: attrs.Index.Get,
			bloom:    filter(dal.SubAttrIndex),
		},
	}
}
