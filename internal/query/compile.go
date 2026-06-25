package query

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// MaxFilterDepth bounds the recursion depth of compile() over
// QueryFilter protos. A hostile client can hand-craft a deeply-nested
// filter (e.g. 100k repetitions of And/Or/Not) and submit it via gRPC;
// without a depth cap the compiler stack-overflows and aborts the
// process (Go stack overflow is not catchable by recover) — a fatal
// DoS. 100 is well above any legitimate query (review-2 L-19 / #341).
const MaxFilterDepth = 100

// ErrFilterTooDeep is returned by Compile when the QueryFilter recursion
// exceeds MaxFilterDepth. Typed Describable (KindValidation) so the gRPC
// adapter maps it to InvalidArgument with the depth in the message.
var ErrFilterTooDeep = domain.NewFilterCompilationError("query filter exceeds maximum nesting depth (%d)", MaxFilterDepth)

// compileCtx holds the immutable context threaded through the recursive
// compilation pipeline. All fields are set once at the entry point and
// read (never mutated) by every sub-function, except depth which is
// incremented by the recursive boolean combinators.
type compileCtx struct {
	kb           *dal.KeyBuilder
	pebbleReader dal.PebbleReader
	indexReader  dal.PebbleReader
	target       commonpb.QueryTarget
	ledgerName   string
	params       map[string]*commonpb.ParameterValue
	schema       map[string]*commonpb.MetadataFieldSchema
	info         *commonpb.LedgerInfo
	// indexRegistry resolves the bucket-scoped Index registry for checkIndexed.
	// Callers wire a Pebble-backed reader (see indexes.NewPebbleReader); a nil
	// reader is treated as "no indexes registered" — every index-bound filter
	// fails with ErrIndexNotFound, which matches the contract for callers that
	// don't carry indexes (tests, ad-hoc compilation outside the server path).
	indexRegistry indexes.Lookup
	// indexVersionFor resolves the per-replica forward-encoding
	// current_version for an IndexID canonical string. The closure
	// MUST read through the same snapshot/reader the iteration uses
	// — otherwise a concurrent atomic version switch can hand the
	// query a version that does not match the snapshot's keyspace.
	// Returns 0 when the local replica has not yet completed a build
	// (BUILDING in the conventional sense), and a non-nil error on
	// read failure. Compile uses it to pick the right v_n keyspace
	// and to refuse early with ErrIndexBuilding when no live keyspace
	// exists yet.
	indexVersionFor func(canonical string) (uint32, error)
	profile         *QueryProfile
	depth           int
}

// metadataCtx holds the per-field context used only by type-specific
// metadata condition compilers (string, int, uint, bool, exists).
type metadataCtx struct {
	prefix    []byte
	entityLen int
	namespace string
	metaKey   string
	// version is the per-replica forward-encoding version resolved
	// by compileFieldCondition for this metadata index. Sub-compilers
	// (compileExistsCondition in particular) use it to build the
	// version-aware eidx prefix.
	version uint32
}

// Compile translates a QueryFilter proto into an EntityIterator tree.
// The params map resolves parameterized conditions at execution time.
// The schema map validates condition types against declared metadata field types;
// a nil schema causes ErrIndexNotFound for any metadata field condition.
// The indexRegistry argument resolves the bucket-scoped Index registry: each
// indexed-read condition (metadata, address role, builtin field) verifies that
// the required Index entry exists and is READY. Pass nil only when no
// index-bound filter is expected (the compiler returns ErrIndexNotFound on the
// first such filter encountered).
// When profile is non-nil, each iterator is wrapped in a TrackedIterator and
// profile.Root is set to the root of the iterator stats tree.
func Compile(
	indexReader dal.PebbleReader,
	kb *dal.KeyBuilder,
	filter *commonpb.QueryFilter,
	target commonpb.QueryTarget,
	ledgerName string,
	params map[string]*commonpb.ParameterValue,
	schema map[string]*commonpb.MetadataFieldSchema,
	info *commonpb.LedgerInfo,
	indexRegistry indexes.Lookup,
	indexVersionFor func(canonical string) (uint32, error),
	profile *QueryProfile,
	pebbleReader dal.PebbleReader,
) (readstore.EntityIterator, error) {
	if indexVersionFor == nil {
		// A nil resolver means "every index is at v=1" — only safe in
		// tests that pre-date EN-1323 versioning. Production wiring
		// always supplies a resolver backed by readstore.
		indexVersionFor = func(string) (uint32, error) { return 1, nil }
	}

	ctx := &compileCtx{
		kb:              kb,
		pebbleReader:    pebbleReader,
		indexReader:     indexReader,
		target:          target,
		ledgerName:      ledgerName,
		params:          params,
		schema:          schema,
		info:            info,
		indexRegistry:   indexRegistry,
		indexVersionFor: indexVersionFor,
		profile:         profile,
	}

	return compile(ctx, filter)
}

// compile is the internal recursive entry point. The depth counter
// guards against malicious deeply-nested QueryFilter protos (#341).
func compile(ctx *compileCtx, filter *commonpb.QueryFilter) (readstore.EntityIterator, error) {
	if filter == nil {
		return compileUniverse(ctx)
	}

	if ctx.depth >= MaxFilterDepth {
		return nil, ErrFilterTooDeep
	}

	ctx.depth++
	defer func() { ctx.depth-- }()

	switch f := filter.GetFilter().(type) {
	case *commonpb.QueryFilter_Field:
		return compileFieldCondition(ctx, f.Field)
	case *commonpb.QueryFilter_Address:
		return compileAddressMatch(ctx, f.Address)
	case *commonpb.QueryFilter_And:
		return compileAnd(ctx, f.And)
	case *commonpb.QueryFilter_Or:
		return compileOr(ctx, f.Or)
	case *commonpb.QueryFilter_Not:
		return compileNot(ctx, f.Not)
	case *commonpb.QueryFilter_Reference:
		return compileReferenceCondition(ctx, f.Reference)
	case *commonpb.QueryFilter_BuiltinUint:
		return compileBuiltinUintCondition(ctx, f.BuiltinUint)
	case *commonpb.QueryFilter_LogBuiltinUint:
		return compileLogBuiltinUintCondition(ctx, f.LogBuiltinUint)
	case *commonpb.QueryFilter_LogId:
		return compileLogIdCondition(ctx, f.LogId.GetCond())
	case *commonpb.QueryFilter_Ledger:
		// Ledger condition is a no-op in the Compile framework --- the ledger
		// is already set in the context. Return the universe iterator.
		return compileUniverse(ctx)
	default:
		return nil, domain.NewFilterCompilationError("unknown filter type: %T", filter.GetFilter())
	}
}

// compileUniverse returns an iterator over ALL entities (no filter).
// For accounts and transactions, reads directly from Pebble (source of truth).
// For logs, reads from the Pebble read index.
func compileUniverse(ctx *compileCtx) (readstore.EntityIterator, error) {
	switch ctx.target {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		iter, err := readstore.NewPebbleAccountIterator(ctx.pebbleReader, ctx.ledgerName)
		if err != nil {
			return nil, fmt.Errorf("creating account iterator: %w", err)
		}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PebbleAccountIterator(%s)", ctx.ledgerName),
			Kind:   "PebbleAccount",
			Prefix: "pebble:attributes",
		}), nil

	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		iter, err := readstore.NewPebbleTxIterator(ctx.pebbleReader, ctx.ledgerName)
		if err != nil {
			return nil, fmt.Errorf("creating tx iterator: %w", err)
		}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PebbleTxIterator(%s)", ctx.ledgerName),
			Kind:   "PebbleTx",
			Prefix: "pebble:txupdate",
		}), nil

	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		iter, err := readstore.NewLedgerLogIterator(ctx.indexReader, ctx.kb, ctx.ledgerName)
		if err != nil {
			return nil, fmt.Errorf("creating log iterator: %w", err)
		}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("LedgerLogIterator(%s)", ctx.ledgerName),
			Kind:   "LedgerLog",
			Prefix: "llog",
		}), nil

	default:
		return &SliceIterator{}, nil
	}
}

// compileAnd compiles an AND filter into a merge-intersect iterator.
func compileAnd(ctx *compileCtx, and *commonpb.AndFilter) (readstore.EntityIterator, error) {
	// Coalesce multiple range predicates on the same metadata field so that
	// `a >= X AND a < Y` compiles to a single bounded IntCondition, not two
	// materialized half-ranges. See mergeFieldRanges for the rules.
	filters := mergeFieldRanges(and.GetFilters())

	children := make([]readstore.EntityIterator, 0, len(filters))

	var childStats []*IteratorStats

	for _, f := range filters {
		child, err := compile(ctx, f)
		if err != nil {
			closeAll(children)

			return nil, err
		}

		if ctx.profile != nil {
			childStats = append(childStats, ctx.profile.Root)
		}

		children = append(children, child)
	}

	if len(children) == 0 {
		return &SliceIterator{}, nil
	}

	if len(children) == 1 {
		return children[0], nil
	}

	andIter := readstore.NewAndIterator(children...)

	stats := &IteratorStats{
		Label:    "AndIterator",
		Kind:     "And",
		Children: childStats,
	}
	if ctx.profile != nil {
		andIter.SetOnSkip(func() { stats.ItemsSkipped++ })
	}

	return trackIterator(andIter, ctx.profile, stats), nil
}

// compileOr compiles an OR filter into a merge-union iterator.
func compileOr(ctx *compileCtx, or *commonpb.OrFilter) (readstore.EntityIterator, error) {
	children := make([]readstore.EntityIterator, 0, len(or.GetFilters()))

	var childStats []*IteratorStats

	for _, f := range or.GetFilters() {
		child, err := compile(ctx, f)
		if err != nil {
			closeAll(children)

			return nil, err
		}

		if ctx.profile != nil {
			childStats = append(childStats, ctx.profile.Root)
		}

		children = append(children, child)
	}

	if len(children) == 0 {
		return &SliceIterator{}, nil
	}

	if len(children) == 1 {
		return children[0], nil
	}

	orIter := readstore.NewOrIterator(children...)

	return trackIterator(orIter, ctx.profile, &IteratorStats{
		Label:    "OrIterator",
		Kind:     "Or",
		Children: childStats,
	}), nil
}

// compileNot compiles a NOT filter into a merge-difference iterator.
func compileNot(ctx *compileCtx, not *commonpb.NotFilter) (readstore.EntityIterator, error) {
	universe, err := compileUniverse(ctx)
	if err != nil {
		return nil, err
	}

	var universeStats *IteratorStats
	if ctx.profile != nil {
		universeStats = ctx.profile.Root
	}

	child, err := compile(ctx, not.GetFilter())
	if err != nil {
		universe.Close()

		return nil, err
	}

	var childStats *IteratorStats
	if ctx.profile != nil {
		childStats = ctx.profile.Root
	}

	notIter := readstore.NewNotIterator(universe, child)

	return trackIterator(notIter, ctx.profile, &IteratorStats{
		Label:    "NotIterator",
		Kind:     "Not",
		Children: []*IteratorStats{universeStats, childStats},
	}), nil
}

// compileFieldCondition compiles a FieldCondition (metadata filter) into a leaf iterator.
func compileFieldCondition(ctx *compileCtx, fc *commonpb.FieldCondition) (readstore.EntityIterator, error) {
	if fc.GetField() == nil {
		return nil, domain.NewFilterCompilationError("field condition has no field reference")
	}

	ns, entityLen := targetNamespaceAndLen(ctx.target)
	metaKey := fc.GetField().GetMetadata()

	// Validate index availability and condition type against declared schema type.
	targetName := targetHumanName(ctx.target)
	if ctx.schema == nil {
		return nil, &domain.BusinessError{Err: &domain.ErrIndexNotFound{Index: fmt.Sprintf("metadata[%q] on %s", metaKey, targetName)}}
	}

	fieldSchema, ok := ctx.schema[metaKey]
	if !ok {
		return nil, &domain.BusinessError{Err: &domain.ErrIndexNotFound{Index: fmt.Sprintf("metadata[%q] on %s", metaKey, targetName)}}
	}

	metaID := indexes.MetadataID(targetTypeForQueryTarget(ctx.target), metaKey)

	indexVersion, err := requireIndexReady(ctx, metaID,
		fmt.Sprintf("metadata[%q] on %s", metaKey, targetName))
	if err != nil {
		return nil, err
	}

	fc, err = validateAndCoerceCondition(fc, fieldSchema)
	if err != nil {
		return nil, err
	}

	mc := &metadataCtx{
		prefix:    readstore.MetadataIndexPrefixV(ctx.kb, ctx.ledgerName, ns, metaKey, indexVersion),
		entityLen: entityLen,
		namespace: ns,
		metaKey:   metaKey,
		version:   indexVersion,
	}

	switch cond := fc.GetCondition().(type) {
	case *commonpb.FieldCondition_StringCond:
		return compileStringCondition(ctx, mc, cond.StringCond)
	case *commonpb.FieldCondition_IntCond:
		return compileIntCondition(ctx, mc, cond.IntCond)
	case *commonpb.FieldCondition_UintCond:
		return compileUintCondition(ctx, mc, cond.UintCond)
	case *commonpb.FieldCondition_BoolCond:
		return compileBoolCondition(ctx, mc, cond.BoolCond)
	case *commonpb.FieldCondition_ExistsCond:
		return compileExistsCondition(ctx, mc, cond.ExistsCond)
	default:
		return nil, domain.NewFilterCompilationError("unknown condition type: %T", fc.GetCondition())
	}
}

// compileStringCondition -- point scan on exact string value.
// Entities are naturally sorted (same value prefix -> entity suffix determines order).
func compileStringCondition(ctx *compileCtx, mc *metadataCtx, cond *commonpb.StringCondition) (readstore.EntityIterator, error) {
	value, err := resolveString(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	fullPrefix := readstore.EncodeString(append([]byte{}, mc.prefix...), value)

	iter, err := readstore.NewPrefixIterator(ctx.indexReader, fullPrefix, len(fullPrefix), mc.entityLen)
	if err != nil {
		return nil, fmt.Errorf("creating string prefix iterator: %w", err)
	}

	return trackIterator(iter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("PrefixIterator(midx:%s:%s:%s=string)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "Prefix",
		Prefix: "midx",
	}), nil
}

// resolvedIntBounds holds resolved min/max bounds for an int64 range condition.
// Values are already adjusted for exclusivity (min incremented if exclusive,
// max incremented if inclusive) so the range is [min, max).
//
// When empty is true, no row can satisfy the condition (e.g. `field > MaxInt64`)
// and the bounds-derived iterator should short-circuit to an empty result.
type resolvedIntBounds struct {
	min    int64
	max    int64
	hasMin bool
	hasMax bool
	empty  bool
}

// isEquality returns true if the resolved bounds cover exactly one value.
// When max == min + 1, the range [min, min+1) matches only value min.
// Within a single value prefix, entities are naturally sorted by entity ID
// in the B+ tree, enabling streaming instead of materializing + sorting.
func (b resolvedIntBounds) isEquality() bool {
	return b.hasMin && b.hasMax && b.max == b.min+1
}

// applyMinExclusive turns `field > v` into `field >= v+1`. Returns (_, false)
// when v == math.MaxInt64 — no int64 satisfies `field > MaxInt64`, so the
// caller marks the bounds empty.
func applyMinExclusive(v int64) (int64, bool) {
	if v == math.MaxInt64 {
		return 0, false
	}

	return v + 1, true
}

// applyMaxInclusive turns `field <= v` into `field < v+1`. Returns (_, false)
// when v == math.MaxInt64 — `field <= MaxInt64` is the entire range, so the
// caller drops the upper bound (hasMax stays false) instead of wrapping.
func applyMaxInclusive(v int64) (int64, bool) {
	if v == math.MaxInt64 {
		return 0, false
	}

	return v + 1, true
}

// resolveIntBounds resolves an IntCondition's bounds from hardcoded values or parameters,
// applying exclusivity adjustments. The returned bounds define a half-open range [min, max).
func resolveIntBounds(cond *commonpb.IntCondition, params map[string]*commonpb.ParameterValue) (resolvedIntBounds, error) {
	var b resolvedIntBounds

	if cond.GetParamMin() != "" {
		v, err := resolveParamInt64(params, cond.GetParamMin())
		if err != nil {
			return b, err
		}

		if cond.GetMinExclusive() {
			nv, ok := applyMinExclusive(v)
			if !ok {
				b.empty = true

				return b, nil
			}

			v = nv
		}

		b.min = v
		b.hasMin = true
	} else if cond.Min != nil {
		v := cond.GetMin()
		if cond.GetMinExclusive() {
			nv, ok := applyMinExclusive(v)
			if !ok {
				b.empty = true

				return b, nil
			}

			v = nv
		}

		b.min = v
		b.hasMin = true
	}

	if cond.GetParamMax() != "" {
		v, err := resolveParamInt64(params, cond.GetParamMax())
		if err != nil {
			return b, err
		}

		if cond.GetMaxExclusive() {
			b.max = v
			b.hasMax = true
		} else if nv, ok := applyMaxInclusive(v); ok {
			b.max = nv
			b.hasMax = true
		}
		// !ok: inclusive max at MaxInt64 means unbounded above — leave hasMax false.
	} else if cond.Max != nil {
		v := cond.GetMax()
		if cond.GetMaxExclusive() {
			b.max = v
			b.hasMax = true
		} else if nv, ok := applyMaxInclusive(v); ok {
			b.max = nv
			b.hasMax = true
		}
	}

	return b, nil
}

// compileIntCondition -- range scan on encoded int64 values.
// For equality conditions (single value), uses streaming PrefixIterator.
// For multi-value ranges, materializes + sorts because entities are not
// sorted by entity ID across different values.
func compileIntCondition(ctx *compileCtx, mc *metadataCtx, cond *commonpb.IntCondition) (readstore.EntityIterator, error) {
	bounds, err := resolveIntBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	if bounds.empty {
		return &SliceIterator{}, nil
	}

	// Equality optimization: single value range -> entities are naturally sorted
	// within the value prefix, so we can stream instead of materializing.
	if bounds.isEquality() {
		fullPrefix := readstore.EncodeInt64(append([]byte{}, mc.prefix...), bounds.min)

		iter, pErr := readstore.NewPrefixIterator(ctx.indexReader, fullPrefix, len(fullPrefix), mc.entityLen)
		if pErr != nil {
			return nil, fmt.Errorf("creating int prefix iterator: %w", pErr)
		}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PrefixIterator(midx:%s:%s:%s=int)", ctx.ledgerName, mc.namespace, mc.metaKey),
			Kind:   "Prefix",
			Prefix: "midx",
		}), nil
	}

	// General range: materialize + sort
	lower := make([]byte, 0, len(mc.prefix)+9)
	lower = append(lower, mc.prefix...)
	upper := make([]byte, 0, len(mc.prefix)+9)
	upper = append(upper, mc.prefix...)

	if bounds.hasMin {
		lower = readstore.EncodeInt64(lower, bounds.min)
	} else {
		lower = append(lower, readstore.TypeTagInt)
	}

	if bounds.hasMax {
		upper = readstore.EncodeInt64(upper, bounds.max)
	} else {
		upper = append(upper, readstore.TypeTagInt+1)
	}

	entityOffset := len(mc.prefix) + 1 + 8 // prefix + typeTag(1) + int64(8)

	iter, rErr := readstore.NewRangeIterator(ctx.indexReader, lower, upper, entityOffset, mc.entityLen)
	if rErr != nil {
		return nil, fmt.Errorf("creating int range iterator: %w", rErr)
	}

	stats := &IteratorStats{
		Label:  fmt.Sprintf("SliceIterator(midx:%s:%s:%s=int range)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "Range",
		Prefix: "midx",
	}
	matIter := materializeIterator(iter, ctx.profile, stats)

	return trackIterator(matIter, ctx.profile, stats), nil
}

// resolvedUintBounds holds resolved min/max bounds for a uint64 range condition.
// Values are already adjusted for exclusivity so the range is [min, max).
//
// When empty is true, no row can satisfy the condition (e.g. `field > MaxUint64`).
type resolvedUintBounds struct {
	min    uint64
	max    uint64
	hasMin bool
	hasMax bool
	empty  bool
}

// isEquality returns true if the resolved bounds cover exactly one value.
func (b resolvedUintBounds) isEquality() bool {
	return b.hasMin && b.hasMax && b.max == b.min+1
}

// applyMinExclusiveUint turns `field > v` into `field >= v+1`. Returns
// (_, false) when v == math.MaxUint64.
func applyMinExclusiveUint(v uint64) (uint64, bool) {
	if v == math.MaxUint64 {
		return 0, false
	}

	return v + 1, true
}

// applyMaxInclusiveUint turns `field <= v` into `field < v+1`. Returns
// (_, false) when v == math.MaxUint64 (caller drops the upper bound).
func applyMaxInclusiveUint(v uint64) (uint64, bool) {
	if v == math.MaxUint64 {
		return 0, false
	}

	return v + 1, true
}

// resolveUintBounds resolves a UintCondition's bounds from hardcoded values or parameters,
// applying exclusivity adjustments. The returned bounds define a half-open range [min, max).
func resolveUintBounds(cond *commonpb.UintCondition, params map[string]*commonpb.ParameterValue) (resolvedUintBounds, error) {
	var b resolvedUintBounds

	if cond.GetParamMin() != "" {
		v, err := resolveParamUint64(params, cond.GetParamMin())
		if err != nil {
			return b, err
		}

		if cond.GetMinExclusive() {
			nv, ok := applyMinExclusiveUint(v)
			if !ok {
				b.empty = true

				return b, nil
			}

			v = nv
		}

		b.min = v
		b.hasMin = true
	} else if cond.Min != nil {
		v := cond.GetMin()
		if cond.GetMinExclusive() {
			nv, ok := applyMinExclusiveUint(v)
			if !ok {
				b.empty = true

				return b, nil
			}

			v = nv
		}

		b.min = v
		b.hasMin = true
	}

	if cond.GetParamMax() != "" {
		v, err := resolveParamUint64(params, cond.GetParamMax())
		if err != nil {
			return b, err
		}

		if cond.GetMaxExclusive() {
			b.max = v
			b.hasMax = true
		} else if nv, ok := applyMaxInclusiveUint(v); ok {
			b.max = nv
			b.hasMax = true
		}
	} else if cond.Max != nil {
		v := cond.GetMax()
		if cond.GetMaxExclusive() {
			b.max = v
			b.hasMax = true
		} else if nv, ok := applyMaxInclusiveUint(v); ok {
			b.max = nv
			b.hasMax = true
		}
	}

	return b, nil
}

// compileUintCondition -- range scan on encoded uint64 values.
// For equality conditions, uses streaming PrefixIterator (same optimization as int).
// For multi-value ranges, materializes + sorts.
func compileUintCondition(ctx *compileCtx, mc *metadataCtx, cond *commonpb.UintCondition) (readstore.EntityIterator, error) {
	bounds, err := resolveUintBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	if bounds.empty {
		return &SliceIterator{}, nil
	}

	// Equality optimization: single value range -> streaming
	if bounds.isEquality() {
		fullPrefix := readstore.EncodeUint64(append([]byte{}, mc.prefix...), bounds.min)

		iter, pErr := readstore.NewPrefixIterator(ctx.indexReader, fullPrefix, len(fullPrefix), mc.entityLen)
		if pErr != nil {
			return nil, fmt.Errorf("creating uint prefix iterator: %w", pErr)
		}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PrefixIterator(midx:%s:%s:%s=uint)", ctx.ledgerName, mc.namespace, mc.metaKey),
			Kind:   "Prefix",
			Prefix: "midx",
		}), nil
	}

	// General range: materialize + sort
	lower := make([]byte, 0, len(mc.prefix)+9)
	lower = append(lower, mc.prefix...)
	upper := make([]byte, 0, len(mc.prefix)+9)
	upper = append(upper, mc.prefix...)

	if bounds.hasMin {
		lower = readstore.EncodeUint64(lower, bounds.min)
	} else {
		lower = append(lower, readstore.TypeTagUint)
	}

	if bounds.hasMax {
		upper = readstore.EncodeUint64(upper, bounds.max)
	} else {
		upper = append(upper, readstore.TypeTagUint+1)
	}

	entityOffset := len(mc.prefix) + 1 + 8

	iter, rErr := readstore.NewRangeIterator(ctx.indexReader, lower, upper, entityOffset, mc.entityLen)
	if rErr != nil {
		return nil, fmt.Errorf("creating uint range iterator: %w", rErr)
	}

	stats := &IteratorStats{
		Label:  fmt.Sprintf("SliceIterator(midx:%s:%s:%s=uint range)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "Range",
		Prefix: "midx",
	}
	matIter := materializeIterator(iter, ctx.profile, stats)

	return trackIterator(matIter, ctx.profile, stats), nil
}

// compileBoolCondition -- point scan on exact bool value.
func compileBoolCondition(ctx *compileCtx, mc *metadataCtx, cond *commonpb.BoolCondition) (readstore.EntityIterator, error) {
	value, err := resolveBool(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	fullPrefix := readstore.EncodeBool(append([]byte{}, mc.prefix...), value)

	iter, pErr := readstore.NewPrefixIterator(ctx.indexReader, fullPrefix, len(fullPrefix), mc.entityLen)
	if pErr != nil {
		return nil, fmt.Errorf("creating bool prefix iterator: %w", pErr)
	}

	return trackIterator(iter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("PrefixIterator(midx:%s:%s:%s=bool)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "Prefix",
		Prefix: "midx",
	}), nil
}

// compileExistsCondition -- streaming scan on the entity-ordered existence index (eidx).
// Entities are stored in entity ID order, so no materialization or sorting is needed.
func compileExistsCondition(ctx *compileCtx, mc *metadataCtx, cond *commonpb.ExistsCondition) (readstore.EntityIterator, error) {
	nonNullPrefix := readstore.EntityExistsNonNullPrefixV(ctx.kb, ctx.ledgerName, mc.namespace, mc.metaKey, mc.version)
	if !cond.GetIncludeNull() {
		// Only non-null entries
		iter, err := readstore.NewPrefixIterator(ctx.indexReader, nonNullPrefix, len(nonNullPrefix), mc.entityLen)
		if err != nil {
			return nil, fmt.Errorf("creating exists non-null prefix iterator: %w", err)
		}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PrefixIterator(eidx:%s:%s:%s non-null)", ctx.ledgerName, mc.namespace, mc.metaKey),
			Kind:   "Prefix",
			Prefix: "eidx",
		}), nil
	}

	// Both non-null and null entries: merge two prefix iterators
	nullPrefix := readstore.EntityExistsNullPrefixV(ctx.kb, ctx.ledgerName, mc.namespace, mc.metaKey, mc.version)

	nonNullIter, err := readstore.NewPrefixIterator(ctx.indexReader, nonNullPrefix, len(nonNullPrefix), mc.entityLen)
	if err != nil {
		return nil, fmt.Errorf("creating exists non-null prefix iterator: %w", err)
	}

	nullIter, err := readstore.NewPrefixIterator(ctx.indexReader, nullPrefix, len(nullPrefix), mc.entityLen)
	if err != nil {
		nonNullIter.Close()

		return nil, fmt.Errorf("creating exists null prefix iterator: %w", err)
	}

	nonNullTracked := trackIterator(nonNullIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("PrefixIterator(eidx:%s:%s:%s non-null)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "Prefix",
		Prefix: "eidx",
	})

	var nonNullStats *IteratorStats
	if ctx.profile != nil {
		nonNullStats = ctx.profile.Root
	}

	nullTracked := trackIterator(nullIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("PrefixIterator(eidx:%s:%s:%s null)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "Prefix",
		Prefix: "eidx",
	})

	var nullStats *IteratorStats
	if ctx.profile != nil {
		nullStats = ctx.profile.Root
	}

	orIter := readstore.NewOrIterator(nonNullTracked, nullTracked)

	return trackIterator(orIter, ctx.profile, &IteratorStats{
		Label:    fmt.Sprintf("OrIterator(eidx:%s:%s:%s exists)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:     "Or",
		Prefix:   "eidx",
		Children: []*IteratorStats{nonNullStats, nullStats},
	}), nil
}

// addressRolePrefix returns the Pebble key prefix byte for the given address role.
func addressRolePrefix(role commonpb.AddressRole) byte {
	switch role {
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		return readstore.PrefixSourceAccountTx
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		return readstore.PrefixDestAccountTx
	default:
		return readstore.PrefixAccountTx
	}
}

// addressRoleBucketLabel returns the display label for the given address role bucket.
func addressRoleBucketLabel(role commonpb.AddressRole) string {
	switch role {
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		return "satx"
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		return "datx"
	default:
		return "atxm"
	}
}

// compileAddressMatch compiles an address filter.
func compileAddressMatch(ctx *compileCtx, am *commonpb.AddressMatch) (readstore.EntityIterator, error) {
	role := am.GetRole()

	// Address filtering on TRANSACTIONS target requires the account-tx index.
	// For ACCOUNTS target, address matching uses the existence index (always on).
	if ctx.target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		id, label := txAddressIndexID(role)
		if _, err := requireIndexReady(ctx, id, label); err != nil {
			return nil, err
		}
	}

	switch m := am.GetMatch().(type) {
	case *commonpb.AddressMatch_HardcodedPrefix:
		return compileAddressPrefix(ctx, m.HardcodedPrefix, role)
	case *commonpb.AddressMatch_HardcodedExact:
		return compileAddressExact(ctx, m.HardcodedExact, role)
	case *commonpb.AddressMatch_ParamPrefix:
		value, err := extractString(ctx.params, m.ParamPrefix)
		if err != nil {
			return nil, err
		}

		return compileAddressPrefix(ctx, value, role)
	case *commonpb.AddressMatch_ParamExact:
		value, err := extractString(ctx.params, m.ParamExact)
		if err != nil {
			return nil, err
		}

		return compileAddressExact(ctx, value, role)
	default:
		return nil, domain.NewFilterCompilationError("unknown address match type: %T", am.GetMatch())
	}
}

func compileAddressPrefix(ctx *compileCtx, addrPrefix string, role commonpb.AddressRole) (readstore.EntityIterator, error) {
	accountIter, err := readstore.NewPebbleAccountPrefixIterator(ctx.pebbleReader, ctx.ledgerName, addrPrefix)
	if err != nil {
		return nil, fmt.Errorf("creating account prefix iterator: %w", err)
	}

	trackedAccount := trackIterator(accountIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("PebbleAccountIterator(%s:%s*)", ctx.ledgerName, addrPrefix),
		Kind:   "PebbleAccount",
		Prefix: "pebble:attributes",
	})

	if ctx.target == commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		return trackedAccount, nil
	}
	// TRANSACTIONS target: translate matching accounts -> transaction IDs
	var accountStats *IteratorStats
	if ctx.profile != nil {
		accountStats = ctx.profile.Root
	}

	addrTxIter := readstore.NewAddressTxIterator(ctx.indexReader, ctx.kb, ctx.ledgerName, trackedAccount, addressRolePrefix(role))

	return trackIterator(addrTxIter, ctx.profile, &IteratorStats{
		Label:    fmt.Sprintf("AddressTxIterator(%s)", ctx.ledgerName),
		Kind:     "AddressTx",
		Prefix:   addressRoleBucketLabel(role),
		Children: []*IteratorStats{accountStats},
	}), nil
}

func compileAddressExact(ctx *compileCtx, exactAddr string, role commonpb.AddressRole) (readstore.EntityIterator, error) {
	// Check if the exact account exists in Pebble by looking for any attribute key
	// with prefix [0xF1][ledger\x00][address\x00]
	exists, err := pebbleAccountExists(ctx.pebbleReader, ctx.ledgerName, exactAddr)
	if err != nil {
		return nil, fmt.Errorf("checking account existence: %w", err)
	}

	if !exists {
		return &SliceIterator{}, nil
	}

	if ctx.target == commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		iter := &SliceIterator{entities: [][]byte{[]byte(exactAddr)}}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label: fmt.Sprintf("SliceIterator(exact:%s)", exactAddr),
			Kind:  "Slice",
		}), nil
	}
	// TRANSACTIONS target: wrap single account in AddressTxIterator
	singleIter := &SliceIterator{entities: [][]byte{[]byte(exactAddr)}}
	trackedSingle := trackIterator(singleIter, ctx.profile, &IteratorStats{
		Label: fmt.Sprintf("SliceIterator(exact:%s)", exactAddr),
		Kind:  "Slice",
	})

	var singleStats *IteratorStats
	if ctx.profile != nil {
		singleStats = ctx.profile.Root
	}

	addrTxIter := readstore.NewAddressTxIterator(ctx.indexReader, ctx.kb, ctx.ledgerName, trackedSingle, addressRolePrefix(role))

	return trackIterator(addrTxIter, ctx.profile, &IteratorStats{
		Label:    fmt.Sprintf("AddressTxIterator(%s)", ctx.ledgerName),
		Kind:     "AddressTx",
		Prefix:   addressRoleBucketLabel(role),
		Children: []*IteratorStats{singleStats},
	}), nil
}

// --- Builtin filters ---

// compileReferenceCondition compiles a ReferenceCondition into a prefix scan on the transaction reference index.
// Requires the reference builtin index to be READY.
func compileReferenceCondition(ctx *compileCtx, rc *commonpb.ReferenceCondition) (readstore.EntityIterator, error) {
	if rc.GetCond() == nil {
		return nil, domain.NewFilterCompilationError("reference condition has no value")
	}

	if _, err := requireIndexReady(ctx,
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
		"reference"); err != nil {
		return nil, err
	}

	value, err := resolveString(rc.GetCond(), ctx.params)
	if err != nil {
		return nil, err
	}

	prefix := readstore.TransactionReferencePrefix(ctx.kb, ctx.ledgerName, value)

	iter, pErr := readstore.NewPrefixIterator(ctx.indexReader, prefix, len(prefix), 8)
	if pErr != nil {
		return nil, fmt.Errorf("creating reference prefix iterator: %w", pErr)
	}

	return trackIterator(iter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("PrefixIterator(txref:%s:%s)", ctx.ledgerName, value),
		Kind:   "Prefix",
		Prefix: "txref",
	}), nil
}

// compileBuiltinUintCondition dispatches to the appropriate builtin uint condition compiler.
func compileBuiltinUintCondition(ctx *compileCtx, cond *commonpb.BuiltinUintCondition) (readstore.EntityIterator, error) {
	if cond.GetCond() == nil {
		return nil, domain.NewFilterCompilationError("builtin uint condition has no value")
	}

	switch cond.GetField() {
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID:
		return compileTxIDCondition(ctx, cond.GetCond())
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
		return compileTimestampCondition(ctx, cond.GetCond())
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
		return compileInsertedAtCondition(ctx, cond.GetCond())
	default:
		return nil, domain.NewFilterCompilationError("unsupported builtin uint field: %v", cond.GetField())
	}
}

// compileTxIDCondition filters transactions by ID using Pebble transaction updates.
// No index is required -- the Pebble cold zone is always present and sorted by txID.
func compileTxIDCondition(ctx *compileCtx, cond *commonpb.UintCondition) (readstore.EntityIterator, error) {
	bounds, err := resolveUintBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	// Equality optimization: single txID -> check existence in Pebble and return slice
	if bounds.isEquality() {
		exists, pErr := pebbleTxExists(ctx.pebbleReader, ctx.ledgerName, bounds.min)
		if pErr != nil {
			return nil, fmt.Errorf("checking tx existence: %w", pErr)
		}

		if !exists {
			return &SliceIterator{}, nil
		}

		txIDBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(txIDBytes, bounds.min)

		iter := &SliceIterator{entities: [][]byte{txIDBytes}}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("SliceIterator(pebble:%s:tx:id=%d)", ctx.ledgerName, bounds.min),
			Kind:   "Slice",
			Prefix: "pebble:txupdate",
		}), nil
	}

	// Range scan using Pebble
	var lower, upper []byte

	if bounds.hasMin {
		lower = make([]byte, 8)
		binary.BigEndian.PutUint64(lower, bounds.min)
	}

	if bounds.hasMax {
		upper = make([]byte, 8)
		binary.BigEndian.PutUint64(upper, bounds.max)
	}

	rangeIter, pErr := readstore.NewPebbleTxRangeIterator(ctx.pebbleReader, ctx.ledgerName, lower, upper)
	if pErr != nil {
		return nil, fmt.Errorf("creating tx range iterator: %w", pErr)
	}

	return trackIterator(rangeIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("PebbleTxRangeIterator(%s:id range)", ctx.ledgerName),
		Kind:   "PebbleTxRange",
		Prefix: "pebble:txupdate",
	}), nil
}

// compileTimestampCondition filters transactions by timestamp using the transaction timestamp index.
// Requires the timestamp builtin index to be READY.
func compileTimestampCondition(ctx *compileCtx, cond *commonpb.UintCondition) (readstore.EntityIterator, error) {
	if _, err := requireIndexReady(ctx,
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
		"timestamp"); err != nil {
		return nil, err
	}

	return compileTimestampRangeCondition(ctx, cond,
		readstore.TransactionTimestampRangePrefix(ctx.kb, ctx.ledgerName), "tstmp")
}

// compileInsertedAtCondition filters transactions by inserted_at using the transaction inserted_at index.
// Requires the inserted_at builtin index to be READY.
func compileInsertedAtCondition(ctx *compileCtx, cond *commonpb.UintCondition) (readstore.EntityIterator, error) {
	if _, err := requireIndexReady(ctx,
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT),
		"inserted_at"); err != nil {
		return nil, err
	}

	return compileTimestampRangeCondition(ctx, cond,
		readstore.TransactionInsertedAtRangePrefix(ctx.kb, ctx.ledgerName), "txiat")
}

// compileTimestampRangeCondition is the shared logic for timestamp-based range scans.
// It handles both transaction timestamps and log dates using the same key layout:
// [prefix_byte][ledger\x00][timestamp_BE(8B)][entityID_BE(8B)].
func compileTimestampRangeCondition(
	ctx *compileCtx,
	cond *commonpb.UintCondition,
	ledgerPrefix []byte,
	bucketLabel string,
) (readstore.EntityIterator, error) {
	bounds, err := resolveUintBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	// Key layout: [prefix_byte][ledger\x00][timestamp_BE(8B)][entityID_BE(8B)]
	entityOffset := len(ledgerPrefix) + 8
	entityLen := 8

	lower := make([]byte, 0, len(ledgerPrefix)+8)
	lower = append(lower, ledgerPrefix...)
	upper := make([]byte, 0, len(ledgerPrefix)+8)
	upper = append(upper, ledgerPrefix...)

	if bounds.hasMin {
		minBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(minBytes, bounds.min)
		lower = append(lower, minBytes...)
	}

	if bounds.hasMax {
		maxBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(maxBytes, bounds.max)
		upper = append(upper, maxBytes...)
	} else {
		upper = append(upper, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)
	}

	if !bounds.hasMin {
		lower = ledgerPrefix
	}

	iter, rErr := readstore.NewRangeIterator(ctx.indexReader, lower, upper, entityOffset, entityLen)
	if rErr != nil {
		return nil, fmt.Errorf("creating timestamp range iterator: %w", rErr)
	}

	stats := &IteratorStats{
		Label:  fmt.Sprintf("SliceIterator(%s:%s range)", bucketLabel, ctx.ledgerName),
		Kind:   "Range",
		Prefix: bucketLabel,
	}
	matIter := materializeIterator(iter, ctx.profile, stats)

	return trackIterator(matIter, ctx.profile, stats), nil
}

// compileLogBuiltinUintCondition dispatches to the appropriate log builtin uint condition compiler.
func compileLogBuiltinUintCondition(ctx *compileCtx, cond *commonpb.LogBuiltinUintCondition) (readstore.EntityIterator, error) {
	if cond.GetCond() == nil {
		return nil, domain.NewFilterCompilationError("log builtin uint condition has no value")
	}

	switch cond.GetField() {
	case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE:
		return compileLogDateCondition(ctx, cond.GetCond())
	default:
		return nil, domain.NewFilterCompilationError("unsupported log builtin uint field: %v", cond.GetField())
	}
}

// compileLogDateCondition filters logs by date using the ledger log date index.
// Requires the log date builtin index to be READY.
func compileLogDateCondition(ctx *compileCtx, cond *commonpb.UintCondition) (readstore.EntityIterator, error) {
	if _, err := requireIndexReady(ctx,
		indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
		"log date"); err != nil {
		return nil, err
	}

	return compileTimestampRangeCondition(ctx, cond,
		readstore.LedgerLogDateRangePrefix(ctx.kb, ctx.ledgerName), "lldt")
}

// compileLogIdCondition filters logs by ledger-local log ID using the ledger logs index.
func compileLogIdCondition(ctx *compileCtx, cond *commonpb.UintCondition) (readstore.EntityIterator, error) {
	if cond == nil {
		return compileUniverse(ctx)
	}

	bounds, err := resolveUintBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	prefix := readstore.LedgerLogPrefix(ctx.kb, ctx.ledgerName)

	// Equality optimization: single logID -> check existence in the index
	if bounds.isEquality() {
		logIDBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(logIDBytes, bounds.min)
		key := readstore.LedgerLogKey(ctx.kb, ctx.ledgerName, bounds.min)

		// Point lookup in Pebble index
		exists, pErr := pebbleKeyExists(ctx.indexReader, key)
		if pErr != nil {
			return nil, fmt.Errorf("checking log existence: %w", pErr)
		}

		if !exists {
			return &SliceIterator{}, nil
		}

		iter := &SliceIterator{entities: [][]byte{logIDBytes}}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("SliceIterator(llog:%s:id=%d)", ctx.ledgerName, bounds.min),
			Kind:   "Slice",
			Prefix: "llog",
		}), nil
	}

	// Range scan on the ledger logs index
	entityOffset := len(prefix)
	lower := make([]byte, 0, len(prefix)+8)
	lower = append(lower, prefix...)
	upper := make([]byte, 0, len(prefix)+8)
	upper = append(upper, prefix...)

	if bounds.hasMin {
		minBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(minBytes, bounds.min)
		lower = append(lower, minBytes...)
	}

	if bounds.hasMax {
		maxBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(maxBytes, bounds.max)
		upper = append(upper, maxBytes...)
	} else {
		upper = append(upper, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)
	}

	if !bounds.hasMin {
		lower = prefix
	}

	entityLen := 8

	iter, rErr := readstore.NewRangeIterator(ctx.indexReader, lower, upper, entityOffset, entityLen)
	if rErr != nil {
		return nil, fmt.Errorf("creating log ID range iterator: %w", rErr)
	}

	stats := &IteratorStats{
		Label:  fmt.Sprintf("SliceIterator(llog:%s:id range)", ctx.ledgerName),
		Kind:   "Range",
		Prefix: "llog",
	}
	matIter := materializeIterator(iter, ctx.profile, stats)

	return trackIterator(matIter, ctx.profile, stats), nil
}

// checkIndexed verifies that an Index entry exists in the bucket-scoped
// registry for (ctx.ledgerName, id). Returns ErrIndexNotFound when the
// entry is missing.
//
// No BuildStatus check: per EN-1323, BuildStatus is informational only.
// Per-replica readiness is signalled by IndexVersionState — see
// requireIndexReady for the combined declaration + local-readiness
// gate every indexed read should use.
func checkIndexed(ctx *compileCtx, id *commonpb.IndexID, label string) error {
	idx, err := indexes.Find(ctx.indexRegistry, ctx.info.GetName(), id)
	if err != nil {
		return fmt.Errorf("looking up index %q: %w", label, err)
	}

	if idx == nil {
		return &domain.BusinessError{Err: &domain.ErrIndexNotFound{Index: label}}
	}

	return nil
}

// requireIndexReady checks BOTH that the index is declared on the
// ledger AND that the local replica has primed a live keyspace for it
// (CurrentVersion > 0 in the per-replica IndexVersionState). Returns:
//   - ErrIndexNotFound when the index isn't declared.
//   - ErrIndexBuilding when the local replica's initial backfill /
//     rewrite hasn't yet performed an atomic switch. This holds for
//     EVERY index kind — builtin (reference, timestamp, inserted_at,
//     address, log_date) and metadata alike — because handleCreatedIndexLog
//     primes each new index at {CurrentVersion: 0, PendingVersion: 1}
//     and only completeBackfill / processSchemaRewrite flips it past 0.
//   - A wrapped error on Pebble I/O failure (per CLAUDE.md invariant
//     #7 the silent "treat as building" fallback is forbidden).
//
// Returns the resolved CurrentVersion on success — callers that key
// their forward index on the version (metadata) use it; builtin
// callers ignore it. The version comes from `ctx.indexVersionFor`,
// which MUST be bound to the iteration snapshot — never the live
// store — so the gate and the scan observe the same point-in-time
// view of the atomic-switch state.
func requireIndexReady(ctx *compileCtx, id *commonpb.IndexID, label string) (uint32, error) {
	if err := checkIndexed(ctx, id, label); err != nil {
		return 0, err
	}

	v, err := ctx.indexVersionFor(indexes.Canonical(id))
	if err != nil {
		return 0, fmt.Errorf("resolving index version for %s: %w", label, err)
	}

	if v == 0 {
		return 0, &domain.BusinessError{Err: &domain.ErrIndexBuilding{Index: label}}
	}

	return v, nil
}

// targetTypeForQueryTarget maps a QueryTarget to the matching TargetType used
// as a discriminator in MetadataIndexID and metadata schemas. Defaults to
// TARGET_TYPE_LEDGER for query targets without a corresponding metadata
// namespace; callers must not invoke this helper outside the metadata
// condition path.
func targetTypeForQueryTarget(t commonpb.QueryTarget) commonpb.TargetType {
	switch t {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		return commonpb.TargetType_TARGET_TYPE_ACCOUNT
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		return commonpb.TargetType_TARGET_TYPE_TRANSACTION
	default:
		return commonpb.TargetType_TARGET_TYPE_LEDGER
	}
}

// --- Helpers ---

// pebbleAccountExists checks if at least one attribute key exists for the given
// account in Pebble. Checks Volume keys first (most common), falls back to Metadata.
func pebbleAccountExists(reader dal.PebbleReader, ledgerName string, address string) (bool, error) {
	// Check Volume keys: [0xF1][V][ledgerName padded 64B][address][sepVolume].
	canonicalBase := make([]byte, dal.LedgerNameFixedSize+len(address))
	copy(canonicalBase[:dal.LedgerNameFixedSize], ledgerName)
	copy(canonicalBase[dal.LedgerNameFixedSize:], address)

	volPrefix := make([]byte, 2+len(canonicalBase)+1)
	volPrefix[0] = dal.ZoneAttributes
	volPrefix[1] = dal.SubAttrVolume
	copy(volPrefix[2:], canonicalBase)
	volPrefix[2+len(canonicalBase)] = dal.CanonicalKeySepVolume

	volUpper := readstore.IncrementBytes(volPrefix)

	vIter, err := dal.NewBoundedIter(reader, volPrefix, volUpper)
	if err != nil {
		return false, err
	}

	if vIter.First() {
		_ = vIter.Close()

		return true, nil
	}

	_ = vIter.Close()

	// Check Metadata keys: [0xF1][M][ledgerName padded 64B][address][sepMetadata].
	metaPrefix := make([]byte, 2+len(canonicalBase)+1)
	metaPrefix[0] = dal.ZoneAttributes
	metaPrefix[1] = dal.SubAttrMetadata
	copy(metaPrefix[2:], canonicalBase)
	metaPrefix[2+len(canonicalBase)] = dal.CanonicalKeySepMetadata

	metaUpper := readstore.IncrementBytes(metaPrefix)

	mIter, err := dal.NewBoundedIter(reader, metaPrefix, metaUpper)
	if err != nil {
		return false, err
	}

	defer func() { _ = mIter.Close() }()

	return mIter.First(), nil
}

// pebbleTxExists checks if at least one attribute key exists for the given
// transaction in Pebble. Key prefix: [0xF1][T][ledgerName padded 64B][sepTransaction][txID(8B)].
func pebbleTxExists(reader dal.PebbleReader, ledgerName string, txID uint64) (bool, error) {
	prefix := make([]byte, 2+dal.LedgerNameFixedSize+1+8)
	prefix[0] = dal.ZoneAttributes
	prefix[1] = dal.SubAttrTransaction
	copy(prefix[2:2+dal.LedgerNameFixedSize], ledgerName)
	prefix[2+dal.LedgerNameFixedSize] = dal.CanonicalKeySepTransaction
	binary.BigEndian.PutUint64(prefix[2+dal.LedgerNameFixedSize+1:], txID)

	upperBound := readstore.IncrementBytes(prefix)

	iter, err := dal.NewBoundedIter(reader, prefix, upperBound)
	if err != nil {
		return false, err
	}

	defer func() { _ = iter.Close() }()

	return iter.First(), nil
}

// pebbleKeyExists checks if an exact key exists in a Pebble reader.
func pebbleKeyExists(reader dal.PebbleReader, key []byte) (bool, error) {
	upper := readstore.IncrementBytes(key)

	iter, err := dal.NewBoundedIter(reader, key, upper)
	if err != nil {
		return false, err
	}

	defer func() { _ = iter.Close() }()

	if !iter.First() {
		return false, nil
	}

	return bytes.Equal(iter.Key(), key), nil
}

// trackIterator wraps an iterator with a TrackedIterator when profiling is active.
// It also sets profile.Root to the new stats node.
func trackIterator(iter readstore.EntityIterator, profile *QueryProfile, stats *IteratorStats) readstore.EntityIterator {
	if profile == nil {
		return iter
	}

	profile.Root = stats

	return NewTrackedIterator(iter, stats)
}

// targetHumanName returns a human-readable name for a query target.
func targetHumanName(target commonpb.QueryTarget) string {
	switch target {
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		return "transactions"
	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		return "logs"
	default:
		return "accounts"
	}
}

// targetNamespaceAndLen returns the namespace and entity length for a query target.
func targetNamespaceAndLen(target commonpb.QueryTarget) (string, int) {
	switch target {
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		return readstore.NamespaceTransaction, 8
	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		return readstore.NamespaceLog, 8
	default:
		return readstore.NamespaceAccount, 0
	}
}

func resolveString(cond *commonpb.StringCondition, params map[string]*commonpb.ParameterValue) (string, error) {
	switch v := cond.GetValue().(type) {
	case *commonpb.StringCondition_Hardcoded:
		return v.Hardcoded, nil
	case *commonpb.StringCondition_Param:
		return extractString(params, v.Param)
	default:
		return "", domain.NewFilterCompilationError("string condition has no value")
	}
}

func resolveBool(cond *commonpb.BoolCondition, params map[string]*commonpb.ParameterValue) (bool, error) {
	switch v := cond.GetValue().(type) {
	case *commonpb.BoolCondition_Hardcoded:
		return v.Hardcoded, nil
	case *commonpb.BoolCondition_Param:
		return extractBool(params, v.Param)
	default:
		return false, domain.NewFilterCompilationError("bool condition has no value")
	}
}

func resolveParamInt64(params map[string]*commonpb.ParameterValue, name string) (int64, error) {
	return extractInt64(params, name)
}

func resolveParamUint64(params map[string]*commonpb.ParameterValue, name string) (uint64, error) {
	return extractUint64(params, name)
}

// extractString extracts a string parameter, returning a clear error on type mismatch or nil value.
func extractString(params map[string]*commonpb.ParameterValue, name string) (string, error) {
	val, ok := params[name]
	if !ok {
		return "", domain.NewFilterCompilationError("parameter %q not provided", name)
	}

	if val == nil || val.GetValue() == nil {
		return "", domain.NewFilterCompilationError("parameter %q has a nil value, expected string", name)
	}

	switch v := val.GetValue().(type) {
	case *commonpb.ParameterValue_StringValue:
		return v.StringValue, nil
	default:
		return "", domain.NewFilterCompilationError("parameter %q: expected string value, got %s", name, paramTypeName(val))
	}
}

// extractBool extracts a bool parameter, returning a clear error on type mismatch or nil value.
// It also accepts string values that strconv.ParseBool recognises ("true",
// "false", "1", "0", etc.), so a client that doesn't know the target type
// can pass through the safe default of sending strings — see #249.
func extractBool(params map[string]*commonpb.ParameterValue, name string) (bool, error) {
	val, ok := params[name]
	if !ok {
		return false, domain.NewFilterCompilationError("parameter %q not provided", name)
	}

	if val == nil || val.GetValue() == nil {
		return false, domain.NewFilterCompilationError("parameter %q has a nil value, expected bool", name)
	}

	switch v := val.GetValue().(type) {
	case *commonpb.ParameterValue_BoolValue:
		return v.BoolValue, nil
	case *commonpb.ParameterValue_StringValue:
		b, err := strconv.ParseBool(v.StringValue)
		if err != nil {
			return false, domain.NewFilterCompilationError("parameter %q: cannot parse %q as bool: %v", name, v.StringValue, err)
		}

		return b, nil
	default:
		return false, domain.NewFilterCompilationError("parameter %q: expected bool value, got %s", name, paramTypeName(val))
	}
}

// extractInt64 extracts an int64 parameter. It also accepts uint64 values
// that fit in int64, and string values that parse cleanly as int64 (so a
// client that doesn't know the target type can pass through the safe
// default of sending strings — see #249).
func extractInt64(params map[string]*commonpb.ParameterValue, name string) (int64, error) {
	val, ok := params[name]
	if !ok {
		return 0, domain.NewFilterCompilationError("parameter %q not provided", name)
	}

	if val == nil || val.GetValue() == nil {
		return 0, domain.NewFilterCompilationError("parameter %q has a nil value, expected int64", name)
	}

	switch v := val.GetValue().(type) {
	case *commonpb.ParameterValue_Int64Value:
		return v.Int64Value, nil
	case *commonpb.ParameterValue_Uint64Value:
		if v.Uint64Value > math.MaxInt64 {
			return 0, domain.NewFilterCompilationError("parameter %q: uint64 value %d overflows int64", name, v.Uint64Value)
		}

		return int64(v.Uint64Value), nil
	case *commonpb.ParameterValue_StringValue:
		n, err := strconv.ParseInt(v.StringValue, 10, 64)
		if err != nil {
			return 0, domain.NewFilterCompilationError("parameter %q: cannot parse %q as int64: %v", name, v.StringValue, err)
		}

		return n, nil
	default:
		return 0, domain.NewFilterCompilationError("parameter %q: expected int64 value, got %s", name, paramTypeName(val))
	}
}

// extractUint64 extracts a uint64 parameter. It also accepts non-negative
// int64 values and string values that parse cleanly as uint64 (so a client
// that doesn't know the target type can pass through the safe default of
// sending strings — see #249).
func extractUint64(params map[string]*commonpb.ParameterValue, name string) (uint64, error) {
	val, ok := params[name]
	if !ok {
		return 0, domain.NewFilterCompilationError("parameter %q not provided", name)
	}

	if val == nil || val.GetValue() == nil {
		return 0, domain.NewFilterCompilationError("parameter %q has a nil value, expected uint64", name)
	}

	switch v := val.GetValue().(type) {
	case *commonpb.ParameterValue_Uint64Value:
		return v.Uint64Value, nil
	case *commonpb.ParameterValue_Int64Value:
		if v.Int64Value < 0 {
			return 0, domain.NewFilterCompilationError("parameter %q: negative int64 value %d cannot be used as uint64", name, v.Int64Value)
		}

		return uint64(v.Int64Value), nil
	case *commonpb.ParameterValue_StringValue:
		n, err := strconv.ParseUint(v.StringValue, 10, 64)
		if err != nil {
			return 0, domain.NewFilterCompilationError("parameter %q: cannot parse %q as uint64: %v", name, v.StringValue, err)
		}

		return n, nil
	default:
		return 0, domain.NewFilterCompilationError("parameter %q: expected uint64 value, got %s", name, paramTypeName(val))
	}
}

// paramTypeName returns a human-readable name for the type of a ParameterValue.
func paramTypeName(pv *commonpb.ParameterValue) string {
	switch pv.GetValue().(type) {
	case *commonpb.ParameterValue_StringValue:
		return "string"
	case *commonpb.ParameterValue_Int64Value:
		return "int64"
	case *commonpb.ParameterValue_Uint64Value:
		return "uint64"
	case *commonpb.ParameterValue_BoolValue:
		return "bool"
	default:
		return "unknown"
	}
}

// materializeIterator drains a Pebble EntityIterator into a sorted SliceIterator.
// When profile is non-nil, it increments the global MaterializedRanges and
// MaterializedItems counters; when stats is non-nil, it also accumulates the
// same counts on the per-node stats so the iterator-tree dump can attribute
// materialization cost to a specific branch.
func materializeIterator(iter readstore.EntityIterator, profile *QueryProfile, stats *IteratorStats) *SliceIterator {
	if profile != nil {
		profile.MaterializedRanges++
	}

	if stats != nil {
		stats.MaterializedRanges++
	}

	defer iter.Close()

	var entities [][]byte

	for iter.Next() {
		entity := iter.Current()
		cp := make([]byte, len(entity))
		copy(cp, entity)
		entities = append(entities, cp)
	}

	if profile != nil {
		profile.MaterializedItems += len(entities)
	}

	if stats != nil {
		stats.MaterializedItems += len(entities)
	}

	sortEntities(entities)

	return &SliceIterator{entities: entities}
}

func sortEntities(entities [][]byte) {
	sort.Slice(entities, func(i, j int) bool {
		return bytes.Compare(entities[i], entities[j]) < 0
	})
}

func closeAll(iters []readstore.EntityIterator) {
	for _, it := range iters {
		it.Close()
	}
}

// txAddressIndexID maps an AddressRole to the IndexID covering it on the
// transactions target, along with a human-readable label for error messages.
func txAddressIndexID(role commonpb.AddressRole) (*commonpb.IndexID, string) {
	switch role {
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		return indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS), "source"
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		return indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS), "destination"
	default:
		return indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS), "address"
	}
}

// SchemaFieldsForTarget extracts the relevant metadata fields map from a schema
// based on the query target. Returns nil if schema is nil.
func SchemaFieldsForTarget(schema *commonpb.MetadataSchema, target commonpb.QueryTarget) map[string]*commonpb.MetadataFieldSchema {
	if schema == nil {
		return nil
	}

	if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		return schema.GetTransactionFields()
	}

	return schema.GetAccountFields()
}

// validateAndCoerceCondition validates a field condition against the declared schema type.
// It returns the (possibly coerced) condition or an error for incompatible types.
// ExistsCondition is always valid regardless of schema type.
func validateAndCoerceCondition(fc *commonpb.FieldCondition, fieldSchema *commonpb.MetadataFieldSchema) (*commonpb.FieldCondition, error) {
	fieldName := fc.GetField().GetMetadata()
	schemaType := fieldSchema.GetType()

	switch fc.GetCondition().(type) {
	case *commonpb.FieldCondition_ExistsCond:
		return fc, nil

	case *commonpb.FieldCondition_IntCond:
		if commonpb.IsSignedType(schemaType) {
			return fc, nil
		}

		if commonpb.IsUnsignedType(schemaType) {
			return coerceIntToUint(fc)
		}

		return nil, domain.NewFilterCompilationError("field %q is declared as %s, cannot use integer condition", fieldName, schemaType)

	case *commonpb.FieldCondition_UintCond:
		if commonpb.IsUnsignedType(schemaType) {
			return fc, nil
		}

		return nil, domain.NewFilterCompilationError("field %q is declared as %s, cannot use unsigned integer condition", fieldName, schemaType)

	case *commonpb.FieldCondition_StringCond:
		if schemaType == commonpb.MetadataType_METADATA_TYPE_STRING {
			return fc, nil
		}

		return nil, domain.NewFilterCompilationError("field %q is declared as %s, cannot use string condition", fieldName, schemaType)

	case *commonpb.FieldCondition_BoolCond:
		if schemaType == commonpb.MetadataType_METADATA_TYPE_BOOL {
			return fc, nil
		}

		return nil, domain.NewFilterCompilationError("field %q is declared as %s, cannot use bool condition", fieldName, schemaType)

	default:
		return fc, nil
	}
}

// coerceIntToUint converts an IntCondition to a UintCondition for unsigned schema fields.
// Returns an error if any bound is negative.
func coerceIntToUint(fc *commonpb.FieldCondition) (*commonpb.FieldCondition, error) {
	fieldName := fc.GetField().GetMetadata()
	intCond := fc.GetIntCond()

	uintCond := &commonpb.UintCondition{
		MinExclusive: intCond.GetMinExclusive(),
		MaxExclusive: intCond.GetMaxExclusive(),
		ParamMin:     intCond.GetParamMin(),
		ParamMax:     intCond.GetParamMax(),
	}

	if intCond.Min != nil {
		v := intCond.GetMin()
		if v < 0 {
			return nil, domain.NewFilterCompilationError("field %q is unsigned, cannot use negative min bound %d", fieldName, v)
		}

		uv := uint64(v)
		uintCond.Min = &uv
	}

	if intCond.Max != nil {
		v := intCond.GetMax()
		if v < 0 {
			return nil, domain.NewFilterCompilationError("field %q is unsigned, cannot use negative max bound %d", fieldName, v)
		}

		uv := uint64(v)
		uintCond.Max = &uv
	}

	return &commonpb.FieldCondition{
		Field:     fc.GetField(),
		Condition: &commonpb.FieldCondition_UintCond{UintCond: uintCond},
	}, nil
}

// SliceIterator wraps a pre-sorted slice of entity IDs as an EntityIterator.
type SliceIterator struct {
	entities [][]byte
	pos      int
	current  []byte
}

func (it *SliceIterator) Next() bool {
	it.pos++
	if it.pos > len(it.entities) {
		return false
	}
	// pos is 1-indexed after first Next() call (starts at 0, first Next -> pos=1)
	idx := it.pos - 1
	if idx >= len(it.entities) {
		return false
	}

	it.current = it.entities[idx]

	return true
}

func (it *SliceIterator) Current() []byte {
	return it.current
}

func (it *SliceIterator) SeekGE(target []byte) bool {
	idx := sort.Search(len(it.entities), func(i int) bool {
		return bytes.Compare(it.entities[i], target) >= 0
	})
	if idx >= len(it.entities) {
		it.pos = len(it.entities) + 1

		return false
	}

	it.pos = idx + 1 // +1 because Next() increments before reading
	it.current = it.entities[idx]

	return true
}

func (it *SliceIterator) Err() error { return nil }

func (it *SliceIterator) Close() {}

var _ readstore.EntityIterator = (*SliceIterator)(nil)
