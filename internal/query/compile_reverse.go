package query

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Descending compilation (EN-1966).
//
// CompileReverse mirrors Compile node for node and returns a
// readstore.ReverseIterator, so a descending page seeks to its cursor and
// stops after the page lookahead instead of draining every match and
// reversing the result.
//
// Only iterator CONSTRUCTION is duplicated here. Every predicate resolution,
// schema validation, index-readiness gate and bound computation is the
// ascending path's — resolveIntBounds, resolveUintBounds, requireIndexReady,
// validateAndCoerceCondition, mergeFieldRanges, pebbleAccountExists,
// rejectInvalidCondition. A filter that compiles one way therefore compiles
// the other way with the same verdict, and a semantic change lands in both.
//
// Two leaf classes cannot stream backwards and keep the materialization the
// ascending path already pays:
//
//   - value-ordered ranges (int/uint metadata ranges, timestamp and log-date
//     ranges, log-id ranges): the scan surfaces rows in (value, entity) order,
//     so "the next entity below X" is undefined without the sorted result;
//   - the account→transaction address union: its members come from N per-account
//     scans that are each ascending but collectively unordered.
//
// Both are served descending through a borrowed reverse cursor over the ONE
// sorted slice they already build (readstore.ReverseSliceIterator) — never a
// second complete-result copy.

// CompileReverse translates a QueryFilter proto into a descending
// ReverseIterator tree. Arguments mirror Compile exactly; see its doc comment.
func CompileReverse(
	indexReader dal.PebbleReader,
	kb *dal.KeyBuilder,
	filter *commonpb.QueryFilter,
	target commonpb.QueryTarget,
	ledgerName string,
	params map[string]*commonpb.ParameterValue,
	schema map[string]*commonpb.MetadataFieldSchema,
	info *commonpb.LedgerInfo,
	indexRegistry indexes.Lookup,
	indexVersionFor readstore.IndexVersionResolver,
	profile *QueryProfile,
	pebbleReader dal.PebbleReader,
	pin uint64,
) (readstore.ReverseIterator, error) {
	// Same early target guard as Compile: an unsupported target must fail
	// loudly rather than reach compileUniverse's default arm and read as an
	// empty-but-successful page.
	if !isSupportedTarget(target) {
		return nil, domain.NewFilterCompilationError("unsupported query target %v", target)
	}

	if indexVersionFor == nil {
		indexVersionFor = func(string) (readstore.ResolvedIndexVersion, bool, error) {
			return readstore.ResolvedIndexVersion{Version: 1}, true, nil
		}
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
		pin:             pin,
		profile:         profile,
	}

	return compileRev(ctx, filter)
}

// trackReverse wraps a reverse iterator with a TrackedReverseIterator when
// profiling is active, and sets profile.Root — the mirror of trackIterator.
func trackReverse(iter readstore.ReverseIterator, profile *QueryProfile, stats *IteratorStats) readstore.ReverseIterator {
	if profile == nil {
		return iter
	}

	profile.Root = stats

	return NewTrackedReverseIterator(iter, stats)
}

// emptyReverse is the descending unsatisfiable result.
func emptyReverse() readstore.ReverseIterator {
	return readstore.NewReverseSliceIterator(nil)
}

// materializeReverse drains a value-ordered range through the SAME
// materialization the ascending path uses, then hands out a reverse cursor
// over that one sorted slice. No second complete-result collection exists at
// any point — which is what separates this from the drain-and-reverse the
// controller used to do.
func materializeReverse(iter readstore.EntityIterator, profile *QueryProfile, stats *IteratorStats) (readstore.ReverseIterator, error) {
	entities, err := materializeEntities(iter, profile, stats)
	if err != nil {
		return nil, err
	}

	return readstore.NewReverseSliceIterator(entities), nil
}

func closeAllReverse(iters []readstore.ReverseIterator) {
	for _, it := range iters {
		it.Close()
	}
}

// compileRev is the descending twin of compile, including the depth guard and
// the per-target condition validity check.
func compileRev(ctx *compileCtx, filter *commonpb.QueryFilter) (readstore.ReverseIterator, error) {
	if filter == nil {
		return compileUniverseRev(ctx)
	}

	if ctx.depth >= MaxFilterDepth {
		return nil, ErrFilterTooDeep
	}

	ctx.depth++
	defer func() { ctx.depth-- }()

	if err := rejectInvalidCondition(ctx.target, filter); err != nil {
		return nil, err
	}

	switch f := filter.GetFilter().(type) {
	case *commonpb.QueryFilter_Field:
		return compileFieldConditionRev(ctx, f.Field)
	case *commonpb.QueryFilter_Address:
		return compileAddressMatchRev(ctx, f.Address)
	case *commonpb.QueryFilter_And:
		return compileAndRev(ctx, f.And)
	case *commonpb.QueryFilter_Or:
		return compileOrRev(ctx, f.Or)
	case *commonpb.QueryFilter_Not:
		return compileNotRev(ctx, f.Not)
	case *commonpb.QueryFilter_Reference:
		return compileReferenceConditionRev(ctx, f.Reference)
	case *commonpb.QueryFilter_Reverted:
		return compileRevertedConditionRev(ctx, f.Reverted)
	case *commonpb.QueryFilter_AccountHasAsset:
		return compileAccountHasAssetConditionRev(ctx, f.AccountHasAsset)
	case *commonpb.QueryFilter_BuiltinUint:
		return compileBuiltinUintConditionRev(ctx, f.BuiltinUint)
	case *commonpb.QueryFilter_LogBuiltinUint:
		return compileLogBuiltinUintConditionRev(ctx, f.LogBuiltinUint)
	case *commonpb.QueryFilter_LogId:
		return compileLogIdConditionRev(ctx, f.LogId.GetCond())
	case *commonpb.QueryFilter_Ledger:
		return compileLedgerConditionRev(ctx, f.Ledger)
	default:
		return nil, domain.NewFilterCompilationError("unknown filter type: %T", filter.GetFilter())
	}
}

// --- Universe and boolean composition ---

func compileUniverseRev(ctx *compileCtx) (readstore.ReverseIterator, error) {
	switch ctx.target {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		iter, err := readstore.NewPebbleReverseAccountIterator(ctx.pebbleReader, ctx.ledgerName)
		if err != nil {
			return nil, fmt.Errorf("creating reverse account iterator: %w", err)
		}

		return trackReverse(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PebbleReverseAccountIterator(%s)", ctx.ledgerName),
			Kind:   "PebbleReverseAccount",
			Prefix: "pebble:attributes",
		}), nil

	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		iter, err := readstore.NewPebbleReverseTxIterator(ctx.pebbleReader, ctx.ledgerName)
		if err != nil {
			return nil, fmt.Errorf("creating reverse tx iterator: %w", err)
		}

		return trackReverse(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PebbleReverseTxIterator(%s)", ctx.ledgerName),
			Kind:   "PebbleReverseTx",
			Prefix: "pebble:txupdate",
		}), nil

	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		prefix := readstore.LedgerLogPrefix(ctx.kb, ctx.ledgerName)

		iter, err := readstore.NewReversePrefixIterator(ctx.indexReader, prefix, len(prefix), 8)
		if err != nil {
			return nil, fmt.Errorf("creating reverse log iterator: %w", err)
		}

		return trackReverse(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("ReverseLedgerLogIterator(%s)", ctx.ledgerName),
			Kind:   "ReverseLedgerLog",
			Prefix: "llog",
		}), nil

	default:
		// Unreachable: CompileReverse rejects unsupported targets up front.
		return nil, domain.NewFilterCompilationError("unsupported query target %v", ctx.target)
	}
}

// compileLedgerConditionRev mirrors compileLedgerCondition: the condition can
// only name the executing ledger or nothing, and naming another ledger is
// unsatisfiable rather than a silent "all logs".
func compileLedgerConditionRev(ctx *compileCtx, lc *commonpb.LedgerCondition) (readstore.ReverseIterator, error) {
	if lc.GetCond() == nil {
		return nil, domain.NewFilterCompilationError("ledger condition has no value")
	}

	want, err := resolveString(lc.GetCond(), ctx.params)
	if err != nil {
		return nil, err
	}

	if want != ctx.ledgerName {
		return emptyReverse(), nil
	}

	return compileUniverseRev(ctx)
}

func compileAndRev(ctx *compileCtx, and *commonpb.AndFilter) (readstore.ReverseIterator, error) {
	filters := mergeFieldRanges(and.GetFilters())

	children := make([]readstore.ReverseIterator, 0, len(filters))

	var childStats []*IteratorStats

	for _, f := range filters {
		child, err := compileRev(ctx, f)
		if err != nil {
			closeAllReverse(children)

			return nil, err
		}

		if ctx.profile != nil {
			childStats = append(childStats, ctx.profile.Root)
		}

		children = append(children, child)
	}

	if len(children) == 0 {
		return emptyReverse(), nil
	}

	if len(children) == 1 {
		return children[0], nil
	}

	andIter := readstore.NewReverseAndIterator(children...)

	stats := &IteratorStats{
		Label:    "ReverseAndIterator",
		Kind:     "And",
		Children: childStats,
	}
	if ctx.profile != nil {
		andIter.SetOnSkip(func() { stats.ItemsSkipped++ })
	}

	return trackReverse(andIter, ctx.profile, stats), nil
}

func compileOrRev(ctx *compileCtx, or *commonpb.OrFilter) (readstore.ReverseIterator, error) {
	children := make([]readstore.ReverseIterator, 0, len(or.GetFilters()))

	var childStats []*IteratorStats

	for _, f := range or.GetFilters() {
		child, err := compileRev(ctx, f)
		if err != nil {
			closeAllReverse(children)

			return nil, err
		}

		if ctx.profile != nil {
			childStats = append(childStats, ctx.profile.Root)
		}

		children = append(children, child)
	}

	if len(children) == 0 {
		return emptyReverse(), nil
	}

	if len(children) == 1 {
		return children[0], nil
	}

	orIter := readstore.NewReverseOrIterator(children...)

	return trackReverse(orIter, ctx.profile, &IteratorStats{
		Label:    "ReverseOrIterator",
		Kind:     "Or",
		Children: childStats,
	}), nil
}

func compileNotRev(ctx *compileCtx, not *commonpb.NotFilter) (readstore.ReverseIterator, error) {
	universe, err := compileUniverseRev(ctx)
	if err != nil {
		return nil, err
	}

	var universeStats *IteratorStats
	if ctx.profile != nil {
		universeStats = ctx.profile.Root
	}

	child, err := compileRev(ctx, not.GetFilter())
	if err != nil {
		universe.Close()

		return nil, err
	}

	var childStats *IteratorStats
	if ctx.profile != nil {
		childStats = ctx.profile.Root
	}

	notIter := readstore.NewReverseNotIterator(universe, child)

	return trackReverse(notIter, ctx.profile, &IteratorStats{
		Label:    "ReverseNotIterator",
		Kind:     "Not",
		Children: []*IteratorStats{universeStats, childStats},
	}), nil
}

// --- Leaves ---

func compileRevertedConditionRev(ctx *compileCtx, cond *commonpb.RevertedCondition) (readstore.ReverseIterator, error) {
	bs, err := ReadReversionBitset(ctx.pebbleReader, ctx.ledgerName)
	if err != nil {
		return nil, fmt.Errorf("reading reversion bitset: %w", err)
	}

	revertedStats := &IteratorStats{
		Label:  fmt.Sprintf("ReverseBitsetIterator(reversions:%s)", ctx.ledgerName),
		Kind:   "Bitset",
		Prefix: "pebble:reversions",
	}

	if cond.GetValue() {
		return trackReverse(readstore.NewReverseBitsetIterator(bs), ctx.profile, revertedStats), nil
	}

	// value == false → universe \ reverted, mirroring compileNotRev.
	universe, err := compileUniverseRev(ctx)
	if err != nil {
		return nil, err
	}

	var universeStats *IteratorStats
	if ctx.profile != nil {
		universeStats = ctx.profile.Root
	}

	reverted := trackReverse(readstore.NewReverseBitsetIterator(bs), ctx.profile, revertedStats)

	var childStats *IteratorStats
	if ctx.profile != nil {
		childStats = ctx.profile.Root
	}

	notIter := readstore.NewReverseNotIterator(universe, reverted)

	return trackReverse(notIter, ctx.profile, &IteratorStats{
		Label:    "ReverseNotIterator",
		Kind:     "Not",
		Children: []*IteratorStats{universeStats, childStats},
	}), nil
}

// compileFieldConditionRev reuses resolveFieldMetadataCtx — the ascending
// path's schema validation, index-readiness gate, retype-window binding and
// condition coercion — and differs only in the leaf it builds.
func compileFieldConditionRev(ctx *compileCtx, fc *commonpb.FieldCondition) (readstore.ReverseIterator, error) {
	fc, mc, err := resolveFieldMetadataCtx(ctx, fc)
	if err != nil {
		return nil, err
	}

	switch cond := fc.GetCondition().(type) {
	case *commonpb.FieldCondition_StringCond:
		return compileStringConditionRev(ctx, mc, cond.StringCond)
	case *commonpb.FieldCondition_IntCond:
		return compileIntConditionRev(ctx, mc, cond.IntCond)
	case *commonpb.FieldCondition_UintCond:
		return compileUintConditionRev(ctx, mc, cond.UintCond)
	case *commonpb.FieldCondition_BoolCond:
		return compileBoolConditionRev(ctx, mc, cond.BoolCond)
	case *commonpb.FieldCondition_ExistsCond:
		return compileExistsConditionRev(ctx, mc, cond.ExistsCond)
	default:
		return nil, domain.NewFilterCompilationError("unknown condition type: %T", fc.GetCondition())
	}
}

func compileStringConditionRev(ctx *compileCtx, mc *metadataCtx, cond *commonpb.StringCondition) (readstore.ReverseIterator, error) {
	value, err := resolveString(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	fullPrefix := readstore.EncodeString(append([]byte{}, mc.prefix...), value)

	iter, err := readstore.NewReverseEventResolveIterator(ctx.indexReader, fullPrefix, ctx.pin)
	if err != nil {
		return nil, fmt.Errorf("creating reverse string event iterator: %w", err)
	}

	return trackReverse(iter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("ReverseEventResolveIterator(midx:%s:%s:%s=string)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "EventResolve",
		Prefix: "midx",
	}), nil
}

func compileBoolConditionRev(ctx *compileCtx, mc *metadataCtx, cond *commonpb.BoolCondition) (readstore.ReverseIterator, error) {
	value, err := resolveBool(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	fullPrefix := readstore.EncodeBool(append([]byte{}, mc.prefix...), value)

	iter, pErr := readstore.NewReverseEventResolveIterator(ctx.indexReader, fullPrefix, ctx.pin)
	if pErr != nil {
		return nil, fmt.Errorf("creating reverse bool event iterator: %w", pErr)
	}

	return trackReverse(iter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("ReverseEventResolveIterator(midx:%s:%s:%s=bool)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "EventResolve",
		Prefix: "midx",
	}), nil
}

func compileExistsConditionRev(ctx *compileCtx, mc *metadataCtx, cond *commonpb.ExistsCondition) (readstore.ReverseIterator, error) {
	nonNullPrefix := readstore.EntityExistsNonNullPrefixV(ctx.kb, ctx.ledgerName, mc.namespace, mc.metaKey, mc.version)
	if !cond.GetIncludeNull() {
		iter, err := readstore.NewReverseEventResolveIterator(ctx.indexReader, nonNullPrefix, ctx.pin)
		if err != nil {
			return nil, fmt.Errorf("creating reverse exists non-null event iterator: %w", err)
		}

		return trackReverse(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("ReverseEventResolveIterator(eidx:%s:%s:%s non-null)", ctx.ledgerName, mc.namespace, mc.metaKey),
			Kind:   "EventResolve",
			Prefix: "eidx",
		}), nil
	}

	nullPrefix := readstore.EntityExistsNullPrefixV(ctx.kb, ctx.ledgerName, mc.namespace, mc.metaKey, mc.version)

	nonNullIter, err := readstore.NewReverseEventResolveIterator(ctx.indexReader, nonNullPrefix, ctx.pin)
	if err != nil {
		return nil, fmt.Errorf("creating reverse exists non-null event iterator: %w", err)
	}

	nullIter, err := readstore.NewReverseEventResolveIterator(ctx.indexReader, nullPrefix, ctx.pin)
	if err != nil {
		nonNullIter.Close()

		return nil, fmt.Errorf("creating reverse exists null event iterator: %w", err)
	}

	nonNullTracked := trackReverse(nonNullIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("ReverseEventResolveIterator(eidx:%s:%s:%s non-null)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "EventResolve",
		Prefix: "eidx",
	})

	var nonNullStats *IteratorStats
	if ctx.profile != nil {
		nonNullStats = ctx.profile.Root
	}

	nullTracked := trackReverse(nullIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("ReverseEventResolveIterator(eidx:%s:%s:%s null)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "EventResolve",
		Prefix: "eidx",
	})

	var nullStats *IteratorStats
	if ctx.profile != nil {
		nullStats = ctx.profile.Root
	}

	orIter := readstore.NewReverseOrIterator(nonNullTracked, nullTracked)

	return trackReverse(orIter, ctx.profile, &IteratorStats{
		Label:    fmt.Sprintf("ReverseOrIterator(eidx:%s:%s:%s exists)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:     "Or",
		Prefix:   "eidx",
		Children: []*IteratorStats{nonNullStats, nullStats},
	}), nil
}

// compileIntConditionRev streams the equality case (one value prefix, entity
// ordered) and serves the general range from the materialized slice.
func compileIntConditionRev(ctx *compileCtx, mc *metadataCtx, cond *commonpb.IntCondition) (readstore.ReverseIterator, error) {
	bounds, err := resolveIntBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	if bounds.empty {
		return emptyReverse(), nil
	}

	if bounds.isEquality() {
		fullPrefix := readstore.EncodeInt64(append([]byte{}, mc.prefix...), bounds.min)

		iter, pErr := readstore.NewReverseEventResolveIterator(ctx.indexReader, fullPrefix, ctx.pin)
		if pErr != nil {
			return nil, fmt.Errorf("creating reverse int event iterator: %w", pErr)
		}

		return trackReverse(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("ReverseEventResolveIterator(midx:%s:%s:%s=int)", ctx.ledgerName, mc.namespace, mc.metaKey),
			Kind:   "EventResolve",
			Prefix: "midx",
		}), nil
	}

	lower, upper := intRangeBounds(mc, bounds)

	iter, rErr := readstore.NewEventResolveRangeIterator(ctx.indexReader, lower, upper, len(mc.prefix), 1+8, ctx.pin)
	if rErr != nil {
		return nil, fmt.Errorf("creating int range event iterator: %w", rErr)
	}

	stats := &IteratorStats{
		Label:  fmt.Sprintf("ReverseSliceIterator(midx:%s:%s:%s=int range)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "Range",
		Prefix: "midx",
	}

	matIter, err := materializeReverse(iter, ctx.profile, stats)
	if err != nil {
		return nil, err
	}

	return trackReverse(matIter, ctx.profile, stats), nil
}

func compileUintConditionRev(ctx *compileCtx, mc *metadataCtx, cond *commonpb.UintCondition) (readstore.ReverseIterator, error) {
	bounds, err := resolveUintBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	if bounds.empty {
		return emptyReverse(), nil
	}

	if bounds.isEquality() {
		fullPrefix := readstore.EncodeUint64(append([]byte{}, mc.prefix...), bounds.min)

		iter, pErr := readstore.NewReverseEventResolveIterator(ctx.indexReader, fullPrefix, ctx.pin)
		if pErr != nil {
			return nil, fmt.Errorf("creating reverse uint event iterator: %w", pErr)
		}

		return trackReverse(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("ReverseEventResolveIterator(midx:%s:%s:%s=uint)", ctx.ledgerName, mc.namespace, mc.metaKey),
			Kind:   "EventResolve",
			Prefix: "midx",
		}), nil
	}

	lower, upper := uintRangeBounds(mc, bounds)

	iter, rErr := readstore.NewEventResolveRangeIterator(ctx.indexReader, lower, upper, len(mc.prefix), 1+8, ctx.pin)
	if rErr != nil {
		return nil, fmt.Errorf("creating uint range event iterator: %w", rErr)
	}

	stats := &IteratorStats{
		Label:  fmt.Sprintf("ReverseSliceIterator(midx:%s:%s:%s=uint range)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "Range",
		Prefix: "midx",
	}

	matIter, err := materializeReverse(iter, ctx.profile, stats)
	if err != nil {
		return nil, err
	}

	return trackReverse(matIter, ctx.profile, stats), nil
}

func compileAddressMatchRev(ctx *compileCtx, am *commonpb.AddressMatch) (readstore.ReverseIterator, error) {
	role := am.GetRole()

	if ctx.target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		id, label := txAddressIndexID(role)
		if _, err := requireIndexReady(ctx, id, label); err != nil {
			return nil, err
		}
	}

	switch m := am.GetMatch().(type) {
	case *commonpb.AddressMatch_HardcodedPrefix:
		return compileAddressPrefixRev(ctx, m.HardcodedPrefix, role)
	case *commonpb.AddressMatch_HardcodedExact:
		return compileAddressExactRev(ctx, m.HardcodedExact, role)
	case *commonpb.AddressMatch_ParamPrefix:
		value, err := extractString(ctx.params, m.ParamPrefix)
		if err != nil {
			return nil, err
		}

		return compileAddressPrefixRev(ctx, value, role)
	case *commonpb.AddressMatch_ParamExact:
		value, err := extractString(ctx.params, m.ParamExact)
		if err != nil {
			return nil, err
		}

		return compileAddressExactRev(ctx, value, role)
	default:
		return nil, domain.NewFilterCompilationError("unknown address match type: %T", am.GetMatch())
	}
}

func compileAddressPrefixRev(ctx *compileCtx, addrPrefix string, role commonpb.AddressRole) (readstore.ReverseIterator, error) {
	if ctx.target == commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		// Accounts are entity-ordered under the address prefix, so this
		// streams descending.
		iter, err := readstore.NewPebbleReverseAccountPrefixIterator(ctx.pebbleReader, ctx.ledgerName, addrPrefix)
		if err != nil {
			return nil, fmt.Errorf("creating reverse account prefix iterator: %w", err)
		}

		return trackReverse(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PebbleReverseAccountIterator(%s:%s*)", ctx.ledgerName, addrPrefix),
			Kind:   "PebbleReverseAccount",
			Prefix: "pebble:attributes",
		}), nil
	}

	// TRANSACTIONS target: the account→tx union is a materializing fallback,
	// served descending through a borrowed cursor over its one sorted slice.
	accountIter, err := readstore.NewPebbleAccountPrefixIterator(ctx.pebbleReader, ctx.ledgerName, addrPrefix)
	if err != nil {
		return nil, fmt.Errorf("creating account prefix iterator: %w", err)
	}

	trackedAccount := trackIterator(accountIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("PebbleAccountIterator(%s:%s*)", ctx.ledgerName, addrPrefix),
		Kind:   "PebbleAccount",
		Prefix: "pebble:attributes",
	})

	var accountStats *IteratorStats
	if ctx.profile != nil {
		accountStats = ctx.profile.Root
	}

	addrTxIter := readstore.NewReverseAddressTxIterator(ctx.indexReader, ctx.kb, ctx.ledgerName, trackedAccount, addressRolePrefix(role))

	return trackReverse(addrTxIter, ctx.profile, &IteratorStats{
		Label:    fmt.Sprintf("ReverseAddressTxIterator(%s)", ctx.ledgerName),
		Kind:     "AddressTx",
		Prefix:   addressRoleBucketLabel(role),
		Children: []*IteratorStats{accountStats},
	}), nil
}

func compileAddressExactRev(ctx *compileCtx, exactAddr string, role commonpb.AddressRole) (readstore.ReverseIterator, error) {
	exists, err := pebbleAccountExists(ctx.pebbleReader, ctx.ledgerName, exactAddr)
	if err != nil {
		return nil, fmt.Errorf("checking account existence: %w", err)
	}

	if !exists {
		return emptyReverse(), nil
	}

	if ctx.target == commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		iter := readstore.NewReverseSliceIterator([][]byte{[]byte(exactAddr)})

		return trackReverse(iter, ctx.profile, &IteratorStats{
			Label: fmt.Sprintf("ReverseSliceIterator(exact:%s)", exactAddr),
			Kind:  "Slice",
		}), nil
	}

	// TRANSACTIONS target: same materializing union as the prefix form. The
	// ticket keeps exact-address materializing until its own streaming
	// optimization lands.
	singleIter := readstore.NewSliceIterator([][]byte{[]byte(exactAddr)})
	trackedSingle := trackIterator(singleIter, ctx.profile, &IteratorStats{
		Label: fmt.Sprintf("SliceIterator(exact:%s)", exactAddr),
		Kind:  "Slice",
	})

	var singleStats *IteratorStats
	if ctx.profile != nil {
		singleStats = ctx.profile.Root
	}

	addrTxIter := readstore.NewReverseAddressTxIterator(ctx.indexReader, ctx.kb, ctx.ledgerName, trackedSingle, addressRolePrefix(role))

	return trackReverse(addrTxIter, ctx.profile, &IteratorStats{
		Label:    fmt.Sprintf("ReverseAddressTxIterator(%s)", ctx.ledgerName),
		Kind:     "AddressTx",
		Prefix:   addressRoleBucketLabel(role),
		Children: []*IteratorStats{singleStats},
	}), nil
}

func compileReferenceConditionRev(ctx *compileCtx, rc *commonpb.ReferenceCondition) (readstore.ReverseIterator, error) {
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

	iter, pErr := readstore.NewReversePrefixIterator(ctx.indexReader, prefix, len(prefix), 8)
	if pErr != nil {
		return nil, fmt.Errorf("creating reverse reference prefix iterator: %w", pErr)
	}

	return trackReverse(iter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("ReversePrefixIterator(txref:%s:%s)", ctx.ledgerName, value),
		Kind:   "Prefix",
		Prefix: "txref",
	}), nil
}

func compileAccountHasAssetConditionRev(ctx *compileCtx, c *commonpb.AccountHasAssetCondition) (readstore.ReverseIterator, error) {
	if _, err := requireIndexReady(ctx,
		indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET),
		"has asset"); err != nil {
		return nil, err
	}

	if c.GetPrecision() > math.MaxUint8 {
		return nil, domain.NewFilterCompilationError("has asset precision %d exceeds maximum %d", c.GetPrecision(), math.MaxUint8)
	}

	prefix := readstore.AccountByAssetPrefix(ctx.kb, ctx.ledgerName, c.GetAssetBase(), uint8(c.GetPrecision()))

	// Stamp-gated in BOTH directions. See compileAccountHasAssetCondition for
	// why the gate exists; a descending scan without it would serve rows the
	// ascending scan hides.
	iter, pErr := readstore.NewStampGatedReversePrefixIterator(ctx.indexReader, prefix, len(prefix), 0, ctx.pin)
	if pErr != nil {
		return nil, fmt.Errorf("creating reverse has-asset prefix iterator: %w", pErr)
	}

	return trackReverse(iter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("ReversePrefixIterator(abya:%s:%s/%d)", ctx.ledgerName, c.GetAssetBase(), c.GetPrecision()),
		Kind:   "Prefix",
		Prefix: "abya",
	}), nil
}

func compileBuiltinUintConditionRev(ctx *compileCtx, cond *commonpb.BuiltinUintCondition) (readstore.ReverseIterator, error) {
	if cond.GetCond() == nil {
		return nil, domain.NewFilterCompilationError("builtin uint condition has no value")
	}

	if cond.GetField() == commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID {
		return compileTxIDConditionRev(ctx, cond.GetCond())
	}

	// Same resolver as the ascending dispatch: index gate, error label, key
	// prefix, profile bucket and stamp gate all come from one place.
	arm, err := resolveTxTimestampArm(ctx, cond.GetField())
	if err != nil {
		return nil, err
	}

	return compileTimestampRangeConditionRev(ctx, cond.GetCond(), arm.prefix, arm.bucket, arm.stampPin)
}

func compileTxIDConditionRev(ctx *compileCtx, cond *commonpb.UintCondition) (readstore.ReverseIterator, error) {
	bounds, err := resolveUintBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	if bounds.empty {
		return emptyReverse(), nil
	}

	if bounds.isEquality() {
		exists, pErr := pebbleTxExists(ctx.pebbleReader, ctx.ledgerName, bounds.min)
		if pErr != nil {
			return nil, fmt.Errorf("checking tx existence: %w", pErr)
		}

		if !exists {
			return emptyReverse(), nil
		}

		txIDBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(txIDBytes, bounds.min)

		iter := readstore.NewReverseSliceIterator([][]byte{txIDBytes})

		return trackReverse(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("ReverseSliceIterator(pebble:%s:tx:id=%d)", ctx.ledgerName, bounds.min),
			Kind:   "Slice",
			Prefix: "pebble:txupdate",
		}), nil
	}

	// The Pebble transaction zone is keyed by txID, so a bounded id range is
	// entity-ordered and streams descending.
	var lower, upper []byte

	if bounds.hasMin {
		lower = make([]byte, 8)
		binary.BigEndian.PutUint64(lower, bounds.min)
	}

	if bounds.hasMax {
		upper = make([]byte, 8)
		binary.BigEndian.PutUint64(upper, bounds.max)
	}

	rangeIter, pErr := readstore.NewPebbleReverseTxRangeIterator(ctx.pebbleReader, ctx.ledgerName, lower, upper)
	if pErr != nil {
		return nil, fmt.Errorf("creating reverse tx range iterator: %w", pErr)
	}

	return trackReverse(rangeIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("PebbleReverseTxRangeIterator(%s:id range)", ctx.ledgerName),
		Kind:   "PebbleTxRange",
		Prefix: "pebble:txupdate",
	}), nil
}

// compileTimestampRangeConditionRev is the value-ordered fallback: the scan
// surfaces rows in (timestamp, entity) order, so the sorted result is
// unavoidable. Descending reuses it rather than building another one.
func compileTimestampRangeConditionRev(
	ctx *compileCtx,
	cond *commonpb.UintCondition,
	ledgerPrefix []byte,
	bucketLabel string,
	stampPin uint64,
) (readstore.ReverseIterator, error) {
	bounds, err := resolveUintBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	if bounds.empty {
		return emptyReverse(), nil
	}

	lower, upper, entityOffset, entityLen := timestampRangeBounds(ledgerPrefix, bounds)

	iter, rErr := readstore.NewStampGatedRangeIterator(ctx.indexReader, lower, upper, entityOffset, entityLen, stampPin)
	if rErr != nil {
		return nil, fmt.Errorf("creating timestamp range iterator: %w", rErr)
	}

	stats := &IteratorStats{
		Label:  fmt.Sprintf("ReverseSliceIterator(%s:%s range)", bucketLabel, ctx.ledgerName),
		Kind:   "Range",
		Prefix: bucketLabel,
	}

	matIter, err := materializeReverse(iter, ctx.profile, stats)
	if err != nil {
		return nil, err
	}

	return trackReverse(matIter, ctx.profile, stats), nil
}

func compileLogBuiltinUintConditionRev(ctx *compileCtx, cond *commonpb.LogBuiltinUintCondition) (readstore.ReverseIterator, error) {
	if cond.GetCond() == nil {
		return nil, domain.NewFilterCompilationError("log builtin uint condition has no value")
	}

	if cond.GetField() != commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE {
		return nil, domain.NewFilterCompilationError("unsupported log builtin uint field: %v", cond.GetField())
	}

	arm, err := resolveLogDateArm(ctx)
	if err != nil {
		return nil, err
	}

	return compileTimestampRangeConditionRev(ctx, cond.GetCond(), arm.prefix, arm.bucket, arm.stampPin)
}

func compileLogIdConditionRev(ctx *compileCtx, cond *commonpb.UintCondition) (readstore.ReverseIterator, error) {
	if cond == nil {
		return compileUniverseRev(ctx)
	}

	bounds, err := resolveUintBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	if bounds.empty {
		return emptyReverse(), nil
	}

	prefix := readstore.LedgerLogPrefix(ctx.kb, ctx.ledgerName)

	if bounds.isEquality() {
		logIDBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(logIDBytes, bounds.min)
		key := readstore.LedgerLogKey(ctx.kb, ctx.ledgerName, bounds.min)

		exists, pErr := pebbleKeyExists(ctx.indexReader, key)
		if pErr != nil {
			return nil, fmt.Errorf("checking log existence: %w", pErr)
		}

		if !exists {
			return emptyReverse(), nil
		}

		iter := readstore.NewReverseSliceIterator([][]byte{logIDBytes})

		return trackReverse(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("ReverseSliceIterator(llog:%s:id=%d)", ctx.ledgerName, bounds.min),
			Kind:   "Slice",
			Prefix: "llog",
		}), nil
	}

	lower, upper := logIDRangeBounds(prefix, bounds)

	iter, rErr := readstore.NewRangeIterator(ctx.indexReader, lower, upper, len(prefix), 8)
	if rErr != nil {
		return nil, fmt.Errorf("creating log ID range iterator: %w", rErr)
	}

	stats := &IteratorStats{
		Label:  fmt.Sprintf("ReverseSliceIterator(llog:%s:id range)", ctx.ledgerName),
		Kind:   "Range",
		Prefix: "llog",
	}

	matIter, err := materializeReverse(iter, ctx.profile, stats)
	if err != nil {
		return nil, err
	}

	return trackReverse(matIter, ctx.profile, stats), nil
}
