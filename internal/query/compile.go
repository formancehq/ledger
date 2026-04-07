package query

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
)

// compileCtx holds the immutable context threaded through the recursive
// compilation pipeline. All fields are set once at the entry point and
// read (never mutated) by every sub-function.
type compileCtx struct {
	kb            *dal.KeyBuilder
	pebbleReader  dal.PebbleReader
	indexReader   dal.PebbleReader
	target        commonpb.QueryTarget
	ledger        string
	params        map[string]*commonpb.ParameterValue
	schema        map[string]*commonpb.MetadataFieldSchema
	builtinCfg    *commonpb.BuiltinIndexConfig
	logBuiltinCfg *commonpb.LogBuiltinIndexConfig
	profile       *QueryProfile
}

// metadataCtx holds the per-field context used only by type-specific
// metadata condition compilers (string, int, uint, bool, exists).
type metadataCtx struct {
	prefix    []byte
	entityLen int
	namespace string
	metaKey   string
}

// Compile translates a QueryFilter proto into an EntityIterator tree.
// The params map resolves parameterized conditions at execution time.
// The schema map validates condition types against declared metadata field types;
// a nil schema causes ErrIndexNotFound for any metadata field condition.
// The builtinCfg checks that required address/builtin indexes are available;
// a nil builtinCfg causes ErrIndexNotFound for any address filter on transactions.
// The logBuiltinCfg checks that required log builtin indexes are available;
// pass nil when the target is not QUERY_TARGET_LOGS.
// When profile is non-nil, each iterator is wrapped in a TrackedIterator and
// profile.Root is set to the root of the iterator stats tree.
func Compile(
	indexReader dal.PebbleReader,
	kb *dal.KeyBuilder,
	filter *commonpb.QueryFilter,
	target commonpb.QueryTarget,
	ledger string,
	params map[string]*commonpb.ParameterValue,
	schema map[string]*commonpb.MetadataFieldSchema,
	builtinCfg *commonpb.BuiltinIndexConfig,
	logBuiltinCfg *commonpb.LogBuiltinIndexConfig,
	profile *QueryProfile,
	pebbleReader dal.PebbleReader,
) (readstore.EntityIterator, error) {
	ctx := &compileCtx{
		kb:            kb,
		pebbleReader:  pebbleReader,
		indexReader:   indexReader,
		target:        target,
		ledger:        ledger,
		params:        params,
		schema:        schema,
		builtinCfg:    builtinCfg,
		logBuiltinCfg: logBuiltinCfg,
		profile:       profile,
	}

	return compile(ctx, filter)
}

// compile is the internal recursive entry point.
func compile(ctx *compileCtx, filter *commonpb.QueryFilter) (readstore.EntityIterator, error) {
	if filter == nil {
		return compileUniverse(ctx)
	}

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
		return nil, fmt.Errorf("unknown filter type: %T", filter.GetFilter())
	}
}

// compileUniverse returns an iterator over ALL entities (no filter).
// For accounts and transactions, reads directly from Pebble (source of truth).
// For logs, reads from the Pebble read index.
func compileUniverse(ctx *compileCtx) (readstore.EntityIterator, error) {
	switch ctx.target {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		iter, err := readstore.NewPebbleAccountIterator(ctx.pebbleReader, ctx.ledger)
		if err != nil {
			return nil, fmt.Errorf("creating account iterator: %w", err)
		}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PebbleAccountIterator(%s)", ctx.ledger),
			Kind:   "PebbleAccount",
			Prefix: "pebble:attributes",
		}), nil

	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		iter, err := readstore.NewPebbleTxIterator(ctx.pebbleReader, ctx.ledger)
		if err != nil {
			return nil, fmt.Errorf("creating tx iterator: %w", err)
		}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PebbleTxIterator(%s)", ctx.ledger),
			Kind:   "PebbleTx",
			Prefix: "pebble:txupdate",
		}), nil

	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		iter, err := readstore.NewLedgerLogIterator(ctx.indexReader, ctx.kb, ctx.ledger)
		if err != nil {
			return nil, fmt.Errorf("creating log iterator: %w", err)
		}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("LedgerLogIterator(%s)", ctx.ledger),
			Kind:   "LedgerLog",
			Prefix: "llog",
		}), nil

	default:
		return &SliceIterator{}, nil
	}
}

// compileAnd compiles an AND filter into a merge-intersect iterator.
func compileAnd(ctx *compileCtx, and *commonpb.AndFilter) (readstore.EntityIterator, error) {
	children := make([]readstore.EntityIterator, 0, len(and.GetFilters()))

	var childStats []*IteratorStats

	for _, f := range and.GetFilters() {
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

	return trackIterator(andIter, ctx.profile, &IteratorStats{
		Label:    "AndIterator",
		Kind:     "And",
		Children: childStats,
	}), nil
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
		return nil, errors.New("field condition has no field reference")
	}

	ns, entityLen := targetNamespaceAndLen(ctx.target)
	metaKey := fc.GetField().GetMetadata()

	// Validate index availability and condition type against declared schema type.
	targetName := targetHumanName(ctx.target)
	if ctx.schema == nil {
		return nil, &domain.BusinessError{Err: &domain.ErrIndexNotFound{Index: fmt.Sprintf("metadata[%q] on %s", metaKey, targetName)}}
	}

	fieldSchema, ok := ctx.schema[metaKey]
	if !ok || !fieldSchema.GetIndexed() {
		return nil, &domain.BusinessError{Err: &domain.ErrIndexNotFound{Index: fmt.Sprintf("metadata[%q] on %s", metaKey, targetName)}}
	}

	if fieldSchema.GetIndexBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
		return nil, &domain.BusinessError{Err: &domain.ErrIndexBuilding{Index: fmt.Sprintf("metadata[%q] on %s", metaKey, targetName)}}
	}

	var err error

	fc, err = validateAndCoerceCondition(fc, fieldSchema)
	if err != nil {
		return nil, err
	}

	mc := &metadataCtx{
		prefix:    readstore.MetadataIndexPrefix(ctx.kb, ctx.ledger, ns, metaKey),
		entityLen: entityLen,
		namespace: ns,
		metaKey:   metaKey,
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
		return nil, fmt.Errorf("unknown condition type: %T", fc.GetCondition())
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
		Label:  fmt.Sprintf("PrefixIterator(midx:%s:%s:%s=string)", ctx.ledger, mc.namespace, mc.metaKey),
		Kind:   "Prefix",
		Prefix: "midx",
	}), nil
}

// resolvedIntBounds holds resolved min/max bounds for an int64 range condition.
// Values are already adjusted for exclusivity (min incremented if exclusive,
// max incremented if inclusive) so the range is [min, max).
type resolvedIntBounds struct {
	min    int64
	max    int64
	hasMin bool
	hasMax bool
}

// isEquality returns true if the resolved bounds cover exactly one value.
// When max == min + 1, the range [min, min+1) matches only value min.
// Within a single value prefix, entities are naturally sorted by entity ID
// in the B+ tree, enabling streaming instead of materializing + sorting.
func (b resolvedIntBounds) isEquality() bool {
	return b.hasMin && b.hasMax && b.max == b.min+1
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
			v++
		}

		b.min = v
		b.hasMin = true
	} else if cond.Min != nil {
		v := cond.GetMin()
		if cond.GetMinExclusive() {
			v++
		}

		b.min = v
		b.hasMin = true
	}

	if cond.GetParamMax() != "" {
		v, err := resolveParamInt64(params, cond.GetParamMax())
		if err != nil {
			return b, err
		}

		if !cond.GetMaxExclusive() {
			v++
		}

		b.max = v
		b.hasMax = true
	} else if cond.Max != nil {
		v := cond.GetMax()
		if !cond.GetMaxExclusive() {
			v++
		}

		b.max = v
		b.hasMax = true
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

	// Equality optimization: single value range -> entities are naturally sorted
	// within the value prefix, so we can stream instead of materializing.
	if bounds.isEquality() {
		fullPrefix := readstore.EncodeInt64(append([]byte{}, mc.prefix...), bounds.min)

		iter, pErr := readstore.NewPrefixIterator(ctx.indexReader, fullPrefix, len(fullPrefix), mc.entityLen)
		if pErr != nil {
			return nil, fmt.Errorf("creating int prefix iterator: %w", pErr)
		}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PrefixIterator(midx:%s:%s:%s=int)", ctx.ledger, mc.namespace, mc.metaKey),
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

	matIter := materializeIterator(iter, ctx.profile)

	return trackIterator(matIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("SliceIterator(midx:%s:%s:%s=int range)", ctx.ledger, mc.namespace, mc.metaKey),
		Kind:   "Range",
		Prefix: "midx",
	}), nil
}

// resolvedUintBounds holds resolved min/max bounds for a uint64 range condition.
// Values are already adjusted for exclusivity so the range is [min, max).
type resolvedUintBounds struct {
	min    uint64
	max    uint64
	hasMin bool
	hasMax bool
}

// isEquality returns true if the resolved bounds cover exactly one value.
func (b resolvedUintBounds) isEquality() bool {
	return b.hasMin && b.hasMax && b.max == b.min+1
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
			v++
		}

		b.min = v
		b.hasMin = true
	} else if cond.Min != nil {
		v := cond.GetMin()
		if cond.GetMinExclusive() {
			v++
		}

		b.min = v
		b.hasMin = true
	}

	if cond.GetParamMax() != "" {
		v, err := resolveParamUint64(params, cond.GetParamMax())
		if err != nil {
			return b, err
		}

		if !cond.GetMaxExclusive() {
			v++
		}

		b.max = v
		b.hasMax = true
	} else if cond.Max != nil {
		v := cond.GetMax()
		if !cond.GetMaxExclusive() {
			v++
		}

		b.max = v
		b.hasMax = true
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

	// Equality optimization: single value range -> streaming
	if bounds.isEquality() {
		fullPrefix := readstore.EncodeUint64(append([]byte{}, mc.prefix...), bounds.min)

		iter, pErr := readstore.NewPrefixIterator(ctx.indexReader, fullPrefix, len(fullPrefix), mc.entityLen)
		if pErr != nil {
			return nil, fmt.Errorf("creating uint prefix iterator: %w", pErr)
		}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PrefixIterator(midx:%s:%s:%s=uint)", ctx.ledger, mc.namespace, mc.metaKey),
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

	matIter := materializeIterator(iter, ctx.profile)

	return trackIterator(matIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("SliceIterator(midx:%s:%s:%s=uint range)", ctx.ledger, mc.namespace, mc.metaKey),
		Kind:   "Range",
		Prefix: "midx",
	}), nil
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
		Label:  fmt.Sprintf("PrefixIterator(midx:%s:%s:%s=bool)", ctx.ledger, mc.namespace, mc.metaKey),
		Kind:   "Prefix",
		Prefix: "midx",
	}), nil
}

// compileExistsCondition -- streaming scan on the entity-ordered existence index (eidx).
// Entities are stored in entity ID order, so no materialization or sorting is needed.
func compileExistsCondition(ctx *compileCtx, mc *metadataCtx, cond *commonpb.ExistsCondition) (readstore.EntityIterator, error) {
	nonNullPrefix := readstore.EntityExistsNonNullPrefix(ctx.kb, ctx.ledger, mc.namespace, mc.metaKey)
	if !cond.GetIncludeNull() {
		// Only non-null entries
		iter, err := readstore.NewPrefixIterator(ctx.indexReader, nonNullPrefix, len(nonNullPrefix), mc.entityLen)
		if err != nil {
			return nil, fmt.Errorf("creating exists non-null prefix iterator: %w", err)
		}

		return trackIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PrefixIterator(eidx:%s:%s:%s non-null)", ctx.ledger, mc.namespace, mc.metaKey),
			Kind:   "Prefix",
			Prefix: "eidx",
		}), nil
	}

	// Both non-null and null entries: merge two prefix iterators
	nullPrefix := readstore.EntityExistsNullPrefix(ctx.kb, ctx.ledger, mc.namespace, mc.metaKey)

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
		Label:  fmt.Sprintf("PrefixIterator(eidx:%s:%s:%s non-null)", ctx.ledger, mc.namespace, mc.metaKey),
		Kind:   "Prefix",
		Prefix: "eidx",
	})

	var nonNullStats *IteratorStats
	if ctx.profile != nil {
		nonNullStats = ctx.profile.Root
	}

	nullTracked := trackIterator(nullIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("PrefixIterator(eidx:%s:%s:%s null)", ctx.ledger, mc.namespace, mc.metaKey),
		Kind:   "Prefix",
		Prefix: "eidx",
	})

	var nullStats *IteratorStats
	if ctx.profile != nil {
		nullStats = ctx.profile.Root
	}

	orIter := readstore.NewOrIterator(nonNullTracked, nullTracked)

	return trackIterator(orIter, ctx.profile, &IteratorStats{
		Label:    fmt.Sprintf("OrIterator(eidx:%s:%s:%s exists)", ctx.ledger, mc.namespace, mc.metaKey),
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
		err := checkAddressRoleIndexed(ctx.builtinCfg, role)
		if err != nil {
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
		return nil, fmt.Errorf("unknown address match type: %T", am.GetMatch())
	}
}

func compileAddressPrefix(ctx *compileCtx, addrPrefix string, role commonpb.AddressRole) (readstore.EntityIterator, error) {
	accountIter, err := readstore.NewPebbleAccountPrefixIterator(ctx.pebbleReader, ctx.ledger, addrPrefix)
	if err != nil {
		return nil, fmt.Errorf("creating account prefix iterator: %w", err)
	}

	trackedAccount := trackIterator(accountIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("PebbleAccountIterator(%s:%s*)", ctx.ledger, addrPrefix),
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

	addrTxIter := readstore.NewAddressTxIterator(ctx.indexReader, ctx.kb, ctx.ledger, trackedAccount, addressRolePrefix(role))

	return trackIterator(addrTxIter, ctx.profile, &IteratorStats{
		Label:    fmt.Sprintf("AddressTxIterator(%s)", ctx.ledger),
		Kind:     "AddressTx",
		Prefix:   addressRoleBucketLabel(role),
		Children: []*IteratorStats{accountStats},
	}), nil
}

func compileAddressExact(ctx *compileCtx, exactAddr string, role commonpb.AddressRole) (readstore.EntityIterator, error) {
	// Check if the exact account exists in Pebble by looking for any attribute key
	// with prefix [0xF1][ledger\x00][address\x00]
	exists, err := pebbleAccountExists(ctx.pebbleReader, ctx.ledger, exactAddr)
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

	addrTxIter := readstore.NewAddressTxIterator(ctx.indexReader, ctx.kb, ctx.ledger, trackedSingle, addressRolePrefix(role))

	return trackIterator(addrTxIter, ctx.profile, &IteratorStats{
		Label:    fmt.Sprintf("AddressTxIterator(%s)", ctx.ledger),
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
		return nil, errors.New("reference condition has no value")
	}

	if err := checkBuiltinIndexed(ctx.builtinCfg, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE); err != nil {
		return nil, err
	}

	value, err := resolveString(rc.GetCond(), ctx.params)
	if err != nil {
		return nil, err
	}

	prefix := readstore.TransactionReferencePrefix(ctx.kb, ctx.ledger, value)

	iter, pErr := readstore.NewPrefixIterator(ctx.indexReader, prefix, len(prefix), 8)
	if pErr != nil {
		return nil, fmt.Errorf("creating reference prefix iterator: %w", pErr)
	}

	return trackIterator(iter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("PrefixIterator(txref:%s:%s)", ctx.ledger, value),
		Kind:   "Prefix",
		Prefix: "txref",
	}), nil
}

// compileBuiltinUintCondition dispatches to the appropriate builtin uint condition compiler.
func compileBuiltinUintCondition(ctx *compileCtx, cond *commonpb.BuiltinUintCondition) (readstore.EntityIterator, error) {
	if cond.GetCond() == nil {
		return nil, errors.New("builtin uint condition has no value")
	}

	switch cond.GetField() {
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID:
		return compileTxIDCondition(ctx, cond.GetCond())
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
		return compileTimestampCondition(ctx, cond.GetCond())
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
		return compileInsertedAtCondition(ctx, cond.GetCond())
	default:
		return nil, fmt.Errorf("unsupported builtin uint field: %v", cond.GetField())
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
		exists, pErr := pebbleTxExists(ctx.pebbleReader, ctx.ledger, bounds.min)
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
			Label:  fmt.Sprintf("SliceIterator(pebble:%s:tx:id=%d)", ctx.ledger, bounds.min),
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

	rangeIter, pErr := readstore.NewPebbleTxRangeIterator(ctx.pebbleReader, ctx.ledger, lower, upper)
	if pErr != nil {
		return nil, fmt.Errorf("creating tx range iterator: %w", pErr)
	}

	return trackIterator(rangeIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("PebbleTxRangeIterator(%s:id range)", ctx.ledger),
		Kind:   "PebbleTxRange",
		Prefix: "pebble:txupdate",
	}), nil
}

// compileTimestampCondition filters transactions by timestamp using the transaction timestamp index.
// Requires the timestamp builtin index to be READY.
func compileTimestampCondition(ctx *compileCtx, cond *commonpb.UintCondition) (readstore.EntityIterator, error) {
	if err := checkBuiltinIndexed(ctx.builtinCfg, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP); err != nil {
		return nil, err
	}

	return compileTimestampRangeCondition(ctx, cond,
		readstore.TransactionTimestampRangePrefix(ctx.kb, ctx.ledger), "tstmp")
}

// compileInsertedAtCondition filters transactions by inserted_at using the transaction inserted_at index.
// Requires the inserted_at builtin index to be READY.
func compileInsertedAtCondition(ctx *compileCtx, cond *commonpb.UintCondition) (readstore.EntityIterator, error) {
	if err := checkBuiltinIndexed(ctx.builtinCfg, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT); err != nil {
		return nil, err
	}

	return compileTimestampRangeCondition(ctx, cond,
		readstore.TransactionInsertedAtRangePrefix(ctx.kb, ctx.ledger), "txiat")
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

	matIter := materializeIterator(iter, ctx.profile)

	return trackIterator(matIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("SliceIterator(%s:%s range)", bucketLabel, ctx.ledger),
		Kind:   "Range",
		Prefix: bucketLabel,
	}), nil
}

// compileLogBuiltinUintCondition dispatches to the appropriate log builtin uint condition compiler.
func compileLogBuiltinUintCondition(ctx *compileCtx, cond *commonpb.LogBuiltinUintCondition) (readstore.EntityIterator, error) {
	if cond.GetCond() == nil {
		return nil, errors.New("log builtin uint condition has no value")
	}

	switch cond.GetField() {
	case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE:
		return compileLogDateCondition(ctx, cond.GetCond())
	default:
		return nil, fmt.Errorf("unsupported log builtin uint field: %v", cond.GetField())
	}
}

// compileLogDateCondition filters logs by date using the ledger log date index.
// Requires the log date builtin index to be READY.
func compileLogDateCondition(ctx *compileCtx, cond *commonpb.UintCondition) (readstore.EntityIterator, error) {
	if err := checkLogBuiltinIndexed(ctx.logBuiltinCfg, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE); err != nil {
		return nil, err
	}

	return compileTimestampRangeCondition(ctx, cond,
		readstore.LedgerLogDateRangePrefix(ctx.kb, ctx.ledger), "lldt")
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

	prefix := readstore.LedgerLogPrefix(ctx.kb, ctx.ledger)

	// Equality optimization: single logID -> check existence in the index
	if bounds.isEquality() {
		logIDBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(logIDBytes, bounds.min)
		key := readstore.LedgerLogKey(ctx.kb, ctx.ledger, bounds.min)

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
			Label:  fmt.Sprintf("SliceIterator(llog:%s:id=%d)", ctx.ledger, bounds.min),
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

	matIter := materializeIterator(iter, ctx.profile)

	return trackIterator(matIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("SliceIterator(llog:%s:id range)", ctx.ledger),
		Kind:   "Range",
		Prefix: "llog",
	}), nil
}

// checkLogBuiltinIndexed validates that the requested log builtin index is enabled and READY.
func checkLogBuiltinIndexed(cfg *commonpb.LogBuiltinIndexConfig, index commonpb.LogBuiltinIndex) error {
	var (
		enabled bool
		status  commonpb.IndexBuildStatus
		label   string
	)

	switch index {
	case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE:
		label = "log date"

		if cfg != nil {
			enabled, status = cfg.GetDate(), cfg.GetDateStatus()
		}
	default:
		return nil
	}

	if !enabled {
		return &domain.BusinessError{Err: &domain.ErrIndexNotFound{Index: label}}
	}

	if status == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
		return &domain.BusinessError{Err: &domain.ErrIndexBuilding{Index: label}}
	}

	return nil
}

// checkBuiltinIndexed validates that the requested builtin index is enabled and READY.
func checkBuiltinIndexed(cfg *commonpb.BuiltinIndexConfig, index commonpb.TransactionBuiltinIndex) error {
	var (
		enabled bool
		status  commonpb.IndexBuildStatus
		label   string
	)

	switch index {
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE:
		label = "reference"

		if cfg != nil {
			enabled, status = cfg.GetReference(), cfg.GetReferenceStatus()
		}
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
		label = "timestamp"

		if cfg != nil {
			enabled, status = cfg.GetTimestamp(), cfg.GetTimestampStatus()
		}
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
		label = "inserted_at"

		if cfg != nil {
			enabled, status = cfg.GetInsertedAt(), cfg.GetInsertedAtStatus()
		}
	default:
		return nil
	}

	if !enabled {
		return &domain.BusinessError{Err: &domain.ErrIndexNotFound{Index: label}}
	}

	if status == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
		return &domain.BusinessError{Err: &domain.ErrIndexBuilding{Index: label}}
	}

	return nil
}

// --- Helpers ---

// pebbleAccountExists checks if at least one attribute key exists for the given
// account in Pebble. Key prefix: [0xF1][ledger\x00][address\x00].
func pebbleAccountExists(reader dal.PebbleReader, ledger, address string) (bool, error) {
	// Attribute keys use two separators after the address:
	//   0x00 (CanonicalKeySepVolume) and 0x01 (CanonicalKeySepMetadata).
	// Scan [prefix][address\x00] to [prefix][address\x02) to cover both.
	// Build lower and upper bounds directly to avoid appendAssign lint issues.
	// Lower: [0xF1][ledger\x00][address][0x00]
	// Upper: [0xF1][ledger\x00][address][0x02]
	baseLen := 1 + len(ledger) + 1 + len(address)
	lowerBound := make([]byte, baseLen+1)
	lowerBound[0] = dal.KeyPrefixAttributes
	n := 1
	n += copy(lowerBound[n:], ledger)
	lowerBound[n] = 0x00
	n++
	n += copy(lowerBound[n:], address)
	lowerBound[n] = dal.CanonicalKeySepVolume

	upperBound := make([]byte, baseLen+1)
	copy(upperBound, lowerBound)
	upperBound[baseLen] = dal.CanonicalKeySepMetadata + 1 // 0x02

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return false, err
	}

	defer func() { _ = iter.Close() }()

	return iter.First(), nil
}

// pebbleTxExists checks if at least one attribute key exists for the given
// transaction in Pebble. Key prefix: [0xF1][ledger\x00\x02][txID(8B)].
func pebbleTxExists(reader dal.PebbleReader, ledger string, txID uint64) (bool, error) {
	prefix := make([]byte, 1+len(ledger)+1+1+8)
	prefix[0] = dal.KeyPrefixAttributes
	n := 1
	n += copy(prefix[n:], ledger)
	prefix[n] = 0x00
	n++
	prefix[n] = dal.CanonicalKeySepTransaction
	n++
	binary.BigEndian.PutUint64(prefix[n:], txID)

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
		return "", errors.New("string condition has no value")
	}
}

func resolveBool(cond *commonpb.BoolCondition, params map[string]*commonpb.ParameterValue) (bool, error) {
	switch v := cond.GetValue().(type) {
	case *commonpb.BoolCondition_Hardcoded:
		return v.Hardcoded, nil
	case *commonpb.BoolCondition_Param:
		return extractBool(params, v.Param)
	default:
		return false, errors.New("bool condition has no value")
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
		return "", fmt.Errorf("parameter %q not provided", name)
	}

	if val == nil || val.GetValue() == nil {
		return "", fmt.Errorf("parameter %q has a nil value, expected string", name)
	}

	switch v := val.GetValue().(type) {
	case *commonpb.ParameterValue_StringValue:
		return v.StringValue, nil
	default:
		return "", fmt.Errorf("parameter %q: expected string value, got %s", name, paramTypeName(val))
	}
}

// extractBool extracts a bool parameter, returning a clear error on type mismatch or nil value.
func extractBool(params map[string]*commonpb.ParameterValue, name string) (bool, error) {
	val, ok := params[name]
	if !ok {
		return false, fmt.Errorf("parameter %q not provided", name)
	}

	if val == nil || val.GetValue() == nil {
		return false, fmt.Errorf("parameter %q has a nil value, expected bool", name)
	}

	switch v := val.GetValue().(type) {
	case *commonpb.ParameterValue_BoolValue:
		return v.BoolValue, nil
	default:
		return false, fmt.Errorf("parameter %q: expected bool value, got %s", name, paramTypeName(val))
	}
}

// extractInt64 extracts an int64 parameter. It also accepts uint64 values that fit in int64.
func extractInt64(params map[string]*commonpb.ParameterValue, name string) (int64, error) {
	val, ok := params[name]
	if !ok {
		return 0, fmt.Errorf("parameter %q not provided", name)
	}

	if val == nil || val.GetValue() == nil {
		return 0, fmt.Errorf("parameter %q has a nil value, expected int64", name)
	}

	switch v := val.GetValue().(type) {
	case *commonpb.ParameterValue_Int64Value:
		return v.Int64Value, nil
	case *commonpb.ParameterValue_Uint64Value:
		if v.Uint64Value > math.MaxInt64 {
			return 0, fmt.Errorf("parameter %q: uint64 value %d overflows int64", name, v.Uint64Value)
		}

		return int64(v.Uint64Value), nil
	default:
		return 0, fmt.Errorf("parameter %q: expected int64 value, got %s", name, paramTypeName(val))
	}
}

// extractUint64 extracts a uint64 parameter. It also accepts non-negative int64 values.
func extractUint64(params map[string]*commonpb.ParameterValue, name string) (uint64, error) {
	val, ok := params[name]
	if !ok {
		return 0, fmt.Errorf("parameter %q not provided", name)
	}

	if val == nil || val.GetValue() == nil {
		return 0, fmt.Errorf("parameter %q has a nil value, expected uint64", name)
	}

	switch v := val.GetValue().(type) {
	case *commonpb.ParameterValue_Uint64Value:
		return v.Uint64Value, nil
	case *commonpb.ParameterValue_Int64Value:
		if v.Int64Value < 0 {
			return 0, fmt.Errorf("parameter %q: negative int64 value %d cannot be used as uint64", name, v.Int64Value)
		}

		return uint64(v.Int64Value), nil
	default:
		return 0, fmt.Errorf("parameter %q: expected uint64 value, got %s", name, paramTypeName(val))
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
// When profile is non-nil, it increments MaterializedRanges and MaterializedItems.
func materializeIterator(iter readstore.EntityIterator, profile *QueryProfile) *SliceIterator {
	if profile != nil {
		profile.MaterializedRanges++
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

// checkAddressRoleIndexed validates that the requested address role index
// exists and is ready via BuiltinIndexConfig. Returns ErrIndexNotFound when
// builtinCfg is nil (no indexes configured).
func checkAddressRoleIndexed(builtinCfg *commonpb.BuiltinIndexConfig, role commonpb.AddressRole) error {
	var (
		indexed bool
		status  commonpb.IndexBuildStatus
		label   string
	)

	switch role {
	case commonpb.AddressRole_ADDRESS_ROLE_ANY:
		label = "address"

		if builtinCfg != nil {
			indexed, status = builtinCfg.GetAddress(), builtinCfg.GetAddressStatus()
		}
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		label = "source"

		if builtinCfg != nil {
			indexed, status = builtinCfg.GetSourceAddress(), builtinCfg.GetSourceAddressStatus()
		}
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		label = "destination"

		if builtinCfg != nil {
			indexed, status = builtinCfg.GetDestAddress(), builtinCfg.GetDestAddressStatus()
		}
	default:
		return nil
	}

	if !indexed {
		return &domain.BusinessError{Err: &domain.ErrIndexNotFound{Index: label}}
	}

	if status == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
		return &domain.BusinessError{Err: &domain.ErrIndexBuilding{Index: label}}
	}

	return nil
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

		return nil, fmt.Errorf("field %q is declared as %s, cannot use integer condition", fieldName, schemaType)

	case *commonpb.FieldCondition_UintCond:
		if commonpb.IsUnsignedType(schemaType) {
			return fc, nil
		}

		return nil, fmt.Errorf("field %q is declared as %s, cannot use unsigned integer condition", fieldName, schemaType)

	case *commonpb.FieldCondition_StringCond:
		if schemaType == commonpb.MetadataType_METADATA_TYPE_STRING {
			return fc, nil
		}

		return nil, fmt.Errorf("field %q is declared as %s, cannot use string condition", fieldName, schemaType)

	case *commonpb.FieldCondition_BoolCond:
		if schemaType == commonpb.MetadataType_METADATA_TYPE_BOOL {
			return fc, nil
		}

		return nil, fmt.Errorf("field %q is declared as %s, cannot use bool condition", fieldName, schemaType)

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
			return nil, fmt.Errorf("field %q is unsigned, cannot use negative min bound %d", fieldName, v)
		}

		uv := uint64(v)
		uintCond.Min = &uv
	}

	if intCond.Max != nil {
		v := intCond.GetMax()
		if v < 0 {
			return nil, fmt.Errorf("field %q is unsigned, cannot use negative max bound %d", fieldName, v)
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

func (it *SliceIterator) Close() {}

var _ readstore.EntityIterator = (*SliceIterator)(nil)
