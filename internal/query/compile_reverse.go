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

// ReverseEntityIterator is a descending EntityIterator with Close. It is the
// return type of CompileReverse: every node (leaf or composite) owns the
// resources it allocated, so a single Close at the root releases the tree.
type ReverseEntityIterator interface {
	readstore.ReverseIterator
	Close()
}

// CompileReverse translates a QueryFilter proto into a descending iterator
// tree. It is the counterpart of Compile for reverse (newest-first) listing:
// entity-ordered leaves stream in descending order, and the materializing
// fallback leaves (value-ordered ranges, the address→transaction union) expose
// their single sorted result through a reverse view — no second all-result
// collection is built solely to reverse.
//
// All arguments and invariants are identical to Compile; see its doc comment.
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
) (ReverseEntityIterator, error) {
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
		pin:             pin,
		indexVersionFor: indexVersionFor,
		profile:         profile,
	}

	return compileReverse(ctx, filter)
}

func compileReverse(ctx *compileCtx, filter *commonpb.QueryFilter) (ReverseEntityIterator, error) {
	if filter == nil {
		return reverseUniverse(ctx)
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
		return reverseFieldCondition(ctx, f.Field)
	case *commonpb.QueryFilter_Address:
		return reverseAddressMatch(ctx, f.Address)
	case *commonpb.QueryFilter_And:
		return reverseAnd(ctx, f.And)
	case *commonpb.QueryFilter_Or:
		return reverseOr(ctx, f.Or)
	case *commonpb.QueryFilter_Not:
		return reverseNot(ctx, f.Not)
	case *commonpb.QueryFilter_Reference:
		return reverseReferenceCondition(ctx, f.Reference)
	case *commonpb.QueryFilter_Reverted:
		return reverseRevertedCondition(ctx, f.Reverted)
	case *commonpb.QueryFilter_AccountHasAsset:
		return reverseAccountHasAssetCondition(ctx, f.AccountHasAsset)
	case *commonpb.QueryFilter_BuiltinUint:
		return reverseBuiltinUintCondition(ctx, f.BuiltinUint)
	case *commonpb.QueryFilter_LogBuiltinUint:
		return reverseLogBuiltinUintCondition(ctx, f.LogBuiltinUint)
	case *commonpb.QueryFilter_LogId:
		return reverseLogIdCondition(ctx, f.LogId.GetCond())
	case *commonpb.QueryFilter_Ledger:
		return reverseLedgerCondition(ctx, f.Ledger)
	default:
		return nil, domain.NewFilterCompilationError("unknown filter type: %T", filter.GetFilter())
	}
}

// reverseUniverse returns a descending iterator over ALL entities for the
// target, mirroring compileUniverse.
func reverseUniverse(ctx *compileCtx) (ReverseEntityIterator, error) {
	switch ctx.target {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		iter, err := readstore.NewPebbleReverseAccountIterator(ctx.pebbleReader, ctx.ledgerName)
		if err != nil {
			return nil, fmt.Errorf("creating reverse account iterator: %w", err)
		}

		return trackReverseIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PebbleReverseAccountIterator(%s)", ctx.ledgerName),
			Kind:   "PebbleReverseAccount",
			Prefix: "pebble:attributes",
		}), nil

	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		iter, err := readstore.NewPebbleReverseTxIterator(ctx.pebbleReader, ctx.ledgerName)
		if err != nil {
			return nil, fmt.Errorf("creating reverse tx iterator: %w", err)
		}

		return trackReverseIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PebbleReverseTxIterator(%s)", ctx.ledgerName),
			Kind:   "PebbleReverseTx",
			Prefix: "pebble:txupdate",
		}), nil

	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		prefix := readstore.LedgerLogPrefix(ctx.kb, ctx.ledgerName)
		entityOffset := len(prefix)

		iter, err := readstore.NewReversePrefixIterator(ctx.indexReader, prefix, entityOffset, 8)
		if err != nil {
			return nil, fmt.Errorf("creating reverse log iterator: %w", err)
		}

		return trackReverseIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("ReverseLedgerLogIterator(%s)", ctx.ledgerName),
			Kind:   "ReverseLedgerLog",
			Prefix: "llog",
		}), nil

	default:
		return nil, domain.NewFilterCompilationError("unsupported query target %v", ctx.target)
	}
}

// reverseLedgerCondition mirrors compileLedgerCondition for descending order.
func reverseLedgerCondition(ctx *compileCtx, lc *commonpb.LedgerCondition) (ReverseEntityIterator, error) {
	if lc.GetCond() == nil {
		return nil, domain.NewFilterCompilationError("ledger condition has no value")
	}

	want, err := resolveString(lc.GetCond(), ctx.params)
	if err != nil {
		return nil, err
	}

	if want != ctx.ledgerName {
		return readstore.NewReverseSliceIterator(nil), nil
	}

	return reverseUniverse(ctx)
}

func reverseAnd(ctx *compileCtx, and *commonpb.AndFilter) (ReverseEntityIterator, error) {
	filters := mergeFieldRanges(and.GetFilters())

	children := make([]ReverseEntityIterator, 0, len(filters))

	var childStats []*IteratorStats

	for _, f := range filters {
		child, err := compileReverse(ctx, f)
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
		return readstore.NewReverseSliceIterator(nil), nil
	}

	if len(children) == 1 {
		return children[0], nil
	}

	args := make([]interface {
		readstore.ReverseIterator
		Close()
	}, len(children))
	for i := range children {
		args[i] = children[i]
	}

	andIter := readstore.NewReverseAndIterator(args...)

	stats := &IteratorStats{
		Label:    "ReverseAndIterator",
		Kind:     "And",
		Children: childStats,
	}
	if ctx.profile != nil {
		andIter.SetOnSkip(func() { stats.ItemsSkipped++ })
	}

	return trackReverseIterator(andIter, ctx.profile, stats), nil
}

func reverseOr(ctx *compileCtx, or *commonpb.OrFilter) (ReverseEntityIterator, error) {
	children := make([]ReverseEntityIterator, 0, len(or.GetFilters()))

	var childStats []*IteratorStats

	for _, f := range or.GetFilters() {
		child, err := compileReverse(ctx, f)
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
		return readstore.NewReverseSliceIterator(nil), nil
	}

	if len(children) == 1 {
		return children[0], nil
	}

	args := make([]interface {
		readstore.ReverseIterator
		Close()
	}, len(children))
	for i := range children {
		args[i] = children[i]
	}

	orIter := readstore.NewReverseOrIterator(args...)

	return trackReverseIterator(orIter, ctx.profile, &IteratorStats{
		Label:    "ReverseOrIterator",
		Kind:     "Or",
		Children: childStats,
	}), nil
}

func reverseNot(ctx *compileCtx, not *commonpb.NotFilter) (ReverseEntityIterator, error) {
	universe, err := reverseUniverse(ctx)
	if err != nil {
		return nil, err
	}

	var universeStats *IteratorStats
	if ctx.profile != nil {
		universeStats = ctx.profile.Root
	}

	child, err := compileReverse(ctx, not.GetFilter())
	if err != nil {
		universe.Close()

		return nil, err
	}

	var childStats *IteratorStats
	if ctx.profile != nil {
		childStats = ctx.profile.Root
	}

	notIter := readstore.NewReverseNotIterator(universe, child)

	return trackReverseIterator(notIter, ctx.profile, &IteratorStats{
		Label:    "ReverseNotIterator",
		Kind:     "Not",
		Children: []*IteratorStats{universeStats, childStats},
	}), nil
}

// reverseRevertedCondition mirrors compileRevertedCondition for descending.
func reverseRevertedCondition(ctx *compileCtx, cond *commonpb.RevertedCondition) (ReverseEntityIterator, error) {
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
		return trackReverseIterator(readstore.NewReverseBitsetIterator(bs), ctx.profile, revertedStats), nil
	}

	universe, err := reverseUniverse(ctx)
	if err != nil {
		return nil, err
	}

	var universeStats *IteratorStats
	if ctx.profile != nil {
		universeStats = ctx.profile.Root
	}

	reverted := trackReverseIterator(readstore.NewReverseBitsetIterator(bs), ctx.profile, revertedStats)

	var childStats *IteratorStats
	if ctx.profile != nil {
		childStats = ctx.profile.Root
	}

	notIter := readstore.NewReverseNotIterator(universe, reverted)

	return trackReverseIterator(notIter, ctx.profile, &IteratorStats{
		Label:    "ReverseNotIterator",
		Kind:     "Not",
		Children: []*IteratorStats{universeStats, childStats},
	}), nil
}

func reverseFieldCondition(ctx *compileCtx, fc *commonpb.FieldCondition) (ReverseEntityIterator, error) {
	if fc.GetField() == nil {
		return nil, domain.NewFilterCompilationError("field condition has no field reference")
	}

	ns := targetNamespace(ctx.target)
	metaKey := fc.GetField().GetMetadata()

	targetName := targetHumanName(ctx.target)
	if ctx.schema == nil {
		return nil, &domain.BusinessError{Err: &domain.ErrIndexNotFound{Index: fmt.Sprintf("metadata[%q] on %s", metaKey, targetName)}}
	}

	fieldSchema, ok := ctx.schema[metaKey]
	if !ok {
		return nil, &domain.BusinessError{Err: &domain.ErrIndexNotFound{Index: fmt.Sprintf("metadata[%q] on %s", metaKey, targetName)}}
	}

	metaID := indexes.MetadataID(targetTypeForQueryTarget(ctx.target), metaKey)

	resolved, err := requireIndexReady(ctx, metaID,
		fmt.Sprintf("metadata[%q] on %s", metaKey, targetName))
	if err != nil {
		return nil, err
	}

	switch {
	case !resolved.BindingKnown:
	case !resolved.TypeDeclared:
		return nil, &domain.BusinessError{Err: &domain.ErrIndexNotFound{Index: fmt.Sprintf("metadata[%q] on %s", metaKey, targetName)}}
	default:
		fieldSchema = &commonpb.MetadataFieldSchema{Type: resolved.Type}
	}

	fc, err = validateAndCoerceCondition(fc, fieldSchema)
	if err != nil {
		return nil, err
	}

	mc := &metadataCtx{
		prefix:    readstore.MetadataIndexPrefixV(ctx.kb, ctx.ledgerName, ns, metaKey, resolved.Version),
		namespace: ns,
		metaKey:   metaKey,
		version:   resolved.Version,
	}

	switch cond := fc.GetCondition().(type) {
	case *commonpb.FieldCondition_StringCond:
		return reverseStringCondition(ctx, mc, cond.StringCond)
	case *commonpb.FieldCondition_IntCond:
		return reverseIntCondition(ctx, mc, cond.IntCond)
	case *commonpb.FieldCondition_UintCond:
		return reverseUintCondition(ctx, mc, cond.UintCond)
	case *commonpb.FieldCondition_BoolCond:
		return reverseBoolCondition(ctx, mc, cond.BoolCond)
	case *commonpb.FieldCondition_ExistsCond:
		return reverseExistsCondition(ctx, mc, cond.ExistsCond)
	default:
		return nil, domain.NewFilterCompilationError("unknown condition type: %T", fc.GetCondition())
	}
}

func reverseStringCondition(ctx *compileCtx, mc *metadataCtx, cond *commonpb.StringCondition) (ReverseEntityIterator, error) {
	value, err := resolveString(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	fullPrefix := readstore.EncodeString(append([]byte{}, mc.prefix...), value)

	iter, err := readstore.NewReverseEventResolveIterator(ctx.indexReader, fullPrefix, ctx.pin)
	if err != nil {
		return nil, fmt.Errorf("creating reverse string event iterator: %w", err)
	}

	return trackReverseIterator(iter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("ReverseEventResolveIterator(midx:%s:%s:%s=string)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "EventResolve",
		Prefix: "midx",
	}), nil
}

func reverseIntCondition(ctx *compileCtx, mc *metadataCtx, cond *commonpb.IntCondition) (ReverseEntityIterator, error) {
	bounds, err := resolveIntBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	if bounds.empty {
		return readstore.NewReverseSliceIterator(nil), nil
	}

	if bounds.isEquality() {
		fullPrefix := readstore.EncodeInt64(append([]byte{}, mc.prefix...), bounds.min)

		iter, pErr := readstore.NewReverseEventResolveIterator(ctx.indexReader, fullPrefix, ctx.pin)
		if pErr != nil {
			return nil, fmt.Errorf("creating reverse int event iterator: %w", pErr)
		}

		return trackReverseIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("ReverseEventResolveIterator(midx:%s:%s:%s=int)", ctx.ledgerName, mc.namespace, mc.metaKey),
			Kind:   "EventResolve",
			Prefix: "midx",
		}), nil
	}

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

	iter, rErr := readstore.NewEventResolveRangeIterator(ctx.indexReader, lower, upper, len(mc.prefix), 1+8, ctx.pin)
	if rErr != nil {
		return nil, fmt.Errorf("creating int range event iterator: %w", rErr)
	}

	stats := &IteratorStats{
		Label:  fmt.Sprintf("ReverseSliceIterator(midx:%s:%s:%s=int range)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "Range",
		Prefix: "midx",
	}
	matIter, err := materializeIterator(iter, ctx.profile, stats)
	if err != nil {
		return nil, err
	}

	return trackReverseIterator(readstore.NewReverseSliceIterator(matIter.entities), ctx.profile, stats), nil
}

func reverseUintCondition(ctx *compileCtx, mc *metadataCtx, cond *commonpb.UintCondition) (ReverseEntityIterator, error) {
	bounds, err := resolveUintBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	if bounds.empty {
		return readstore.NewReverseSliceIterator(nil), nil
	}

	if bounds.isEquality() {
		fullPrefix := readstore.EncodeUint64(append([]byte{}, mc.prefix...), bounds.min)

		iter, pErr := readstore.NewReverseEventResolveIterator(ctx.indexReader, fullPrefix, ctx.pin)
		if pErr != nil {
			return nil, fmt.Errorf("creating reverse uint event iterator: %w", pErr)
		}

		return trackReverseIterator(iter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("ReverseEventResolveIterator(midx:%s:%s:%s=uint)", ctx.ledgerName, mc.namespace, mc.metaKey),
			Kind:   "EventResolve",
			Prefix: "midx",
		}), nil
	}

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

	iter, rErr := readstore.NewEventResolveRangeIterator(ctx.indexReader, lower, upper, len(mc.prefix), 1+8, ctx.pin)
	if rErr != nil {
		return nil, fmt.Errorf("creating uint range event iterator: %w", rErr)
	}

	stats := &IteratorStats{
		Label:  fmt.Sprintf("ReverseSliceIterator(midx:%s:%s:%s=uint range)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "Range",
		Prefix: "midx",
	}
	matIter, err := materializeIterator(iter, ctx.profile, stats)
	if err != nil {
		return nil, err
	}

	return trackReverseIterator(readstore.NewReverseSliceIterator(matIter.entities), ctx.profile, stats), nil
}

func reverseBoolCondition(ctx *compileCtx, mc *metadataCtx, cond *commonpb.BoolCondition) (ReverseEntityIterator, error) {
	value, err := resolveBool(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	fullPrefix := readstore.EncodeBool(append([]byte{}, mc.prefix...), value)

	iter, pErr := readstore.NewReverseEventResolveIterator(ctx.indexReader, fullPrefix, ctx.pin)
	if pErr != nil {
		return nil, fmt.Errorf("creating reverse bool event iterator: %w", pErr)
	}

	return trackReverseIterator(iter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("ReverseEventResolveIterator(midx:%s:%s:%s=bool)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "EventResolve",
		Prefix: "midx",
	}), nil
}

func reverseExistsCondition(ctx *compileCtx, mc *metadataCtx, cond *commonpb.ExistsCondition) (ReverseEntityIterator, error) {
	nonNullPrefix := readstore.EntityExistsNonNullPrefixV(ctx.kb, ctx.ledgerName, mc.namespace, mc.metaKey, mc.version)
	if !cond.GetIncludeNull() {
		iter, err := readstore.NewReverseEventResolveIterator(ctx.indexReader, nonNullPrefix, ctx.pin)
		if err != nil {
			return nil, fmt.Errorf("creating reverse exists non-null event iterator: %w", err)
		}

		return trackReverseIterator(iter, ctx.profile, &IteratorStats{
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

	nonNullTracked := trackReverseIterator(nonNullIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("ReverseEventResolveIterator(eidx:%s:%s:%s non-null)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "EventResolve",
		Prefix: "eidx",
	})

	var nonNullStats *IteratorStats
	if ctx.profile != nil {
		nonNullStats = ctx.profile.Root
	}

	nullTracked := trackReverseIterator(nullIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("ReverseEventResolveIterator(eidx:%s:%s:%s null)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:   "EventResolve",
		Prefix: "eidx",
	})

	var nullStats *IteratorStats
	if ctx.profile != nil {
		nullStats = ctx.profile.Root
	}

	orIter := readstore.NewReverseOrIterator(nonNullTracked, nullTracked)

	return trackReverseIterator(orIter, ctx.profile, &IteratorStats{
		Label:    fmt.Sprintf("ReverseOrIterator(eidx:%s:%s:%s exists)", ctx.ledgerName, mc.namespace, mc.metaKey),
		Kind:     "Or",
		Prefix:   "eidx",
		Children: []*IteratorStats{nonNullStats, nullStats},
	}), nil
}

func reverseAddressMatch(ctx *compileCtx, am *commonpb.AddressMatch) (ReverseEntityIterator, error) {
	role := am.GetRole()

	if ctx.target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		id, label := txAddressIndexID(role)
		if _, err := requireIndexReady(ctx, id, label); err != nil {
			return nil, err
		}
	}

	switch m := am.GetMatch().(type) {
	case *commonpb.AddressMatch_HardcodedPrefix:
		return reverseAddressPrefix(ctx, m.HardcodedPrefix, role)
	case *commonpb.AddressMatch_HardcodedExact:
		return reverseAddressExact(ctx, m.HardcodedExact, role)
	case *commonpb.AddressMatch_ParamPrefix:
		value, err := extractString(ctx.params, m.ParamPrefix)
		if err != nil {
			return nil, err
		}

		return reverseAddressPrefix(ctx, value, role)
	case *commonpb.AddressMatch_ParamExact:
		value, err := extractString(ctx.params, m.ParamExact)
		if err != nil {
			return nil, err
		}

		return reverseAddressExact(ctx, value, role)
	default:
		return nil, domain.NewFilterCompilationError("unknown address match type: %T", am.GetMatch())
	}
}

func reverseAddressPrefix(ctx *compileCtx, addrPrefix string, role commonpb.AddressRole) (ReverseEntityIterator, error) {
	if ctx.target == commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		accountIter, err := readstore.NewPebbleReverseAccountPrefixIterator(ctx.pebbleReader, ctx.ledgerName, addrPrefix)
		if err != nil {
			return nil, fmt.Errorf("creating reverse account prefix iterator: %w", err)
		}

		return trackReverseIterator(accountIter, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("PebbleReverseAccountIterator(%s:%s*)", ctx.ledgerName, addrPrefix),
			Kind:   "PebbleReverseAccount",
			Prefix: "pebble:attributes",
		}), nil
	}

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

	addrTxIter := readstore.NewAddressTxIterator(ctx.indexReader, ctx.kb, ctx.ledgerName, trackedAccount, addressRolePrefix(role))

	txns, err := addrTxIter.SortedTxns()
	if err != nil {
		addrTxIter.Close()

		return nil, err
	}

	rev := readstore.NewReverseSliceIterator(txns)
	rev.SetClose(addrTxIter.Close)

	return trackReverseIterator(rev, ctx.profile, &IteratorStats{
		Label:    fmt.Sprintf("ReverseAddressTxIterator(%s)", ctx.ledgerName),
		Kind:     "AddressTx",
		Prefix:   addressRoleBucketLabel(role),
		Children: []*IteratorStats{accountStats},
	}), nil
}

func reverseAddressExact(ctx *compileCtx, exactAddr string, role commonpb.AddressRole) (ReverseEntityIterator, error) {
	exists, err := pebbleAccountExists(ctx.pebbleReader, ctx.ledgerName, exactAddr)
	if err != nil {
		return nil, fmt.Errorf("checking account existence: %w", err)
	}

	if !exists {
		return readstore.NewReverseSliceIterator(nil), nil
	}

	if ctx.target == commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		rev := readstore.NewReverseSliceIterator([][]byte{[]byte(exactAddr)})

		return trackReverseIterator(rev, ctx.profile, &IteratorStats{
			Label: fmt.Sprintf("ReverseSliceIterator(exact:%s)", exactAddr),
			Kind:  "Slice",
		}), nil
	}

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

	txns, err := addrTxIter.SortedTxns()
	if err != nil {
		addrTxIter.Close()

		return nil, err
	}

	rev := readstore.NewReverseSliceIterator(txns)
	rev.SetClose(addrTxIter.Close)

	return trackReverseIterator(rev, ctx.profile, &IteratorStats{
		Label:    fmt.Sprintf("ReverseAddressTxIterator(%s)", ctx.ledgerName),
		Kind:     "AddressTx",
		Prefix:   addressRoleBucketLabel(role),
		Children: []*IteratorStats{singleStats},
	}), nil
}

func reverseReferenceCondition(ctx *compileCtx, rc *commonpb.ReferenceCondition) (ReverseEntityIterator, error) {
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

	return trackReverseIterator(iter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("ReversePrefixIterator(txref:%s:%s)", ctx.ledgerName, value),
		Kind:   "Prefix",
		Prefix: "txref",
	}), nil
}

func reverseAccountHasAssetCondition(ctx *compileCtx, c *commonpb.AccountHasAssetCondition) (ReverseEntityIterator, error) {
	if _, err := requireIndexReady(ctx,
		indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET),
		"has asset"); err != nil {
		return nil, err
	}

	if c.GetPrecision() > math.MaxUint8 {
		return nil, domain.NewFilterCompilationError("has asset precision %d exceeds maximum %d", c.GetPrecision(), math.MaxUint8)
	}

	prefix := readstore.AccountByAssetPrefix(ctx.kb, ctx.ledgerName, c.GetAssetBase(), uint8(c.GetPrecision()))

	iter, pErr := readstore.NewStampGatedReversePrefixIterator(ctx.indexReader, prefix, len(prefix), 0, ctx.pin)
	if pErr != nil {
		return nil, fmt.Errorf("creating reverse has-asset prefix iterator: %w", pErr)
	}

	return trackReverseIterator(iter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("ReversePrefixIterator(abya:%s:%s/%d)", ctx.ledgerName, c.GetAssetBase(), c.GetPrecision()),
		Kind:   "Prefix",
		Prefix: "abya",
	}), nil
}

func reverseBuiltinUintCondition(ctx *compileCtx, cond *commonpb.BuiltinUintCondition) (ReverseEntityIterator, error) {
	if cond.GetCond() == nil {
		return nil, domain.NewFilterCompilationError("builtin uint condition has no value")
	}

	switch cond.GetField() {
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID:
		return reverseTxIDCondition(ctx, cond.GetCond())
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
		return reverseTimestampCondition(ctx, cond.GetCond())
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
		return reverseInsertedAtCondition(ctx, cond.GetCond())
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT:
		return reverseRevertedAtCondition(ctx, cond.GetCond())
	default:
		return nil, domain.NewFilterCompilationError("unsupported builtin uint field: %v", cond.GetField())
	}
}

func reverseTxIDCondition(ctx *compileCtx, cond *commonpb.UintCondition) (ReverseEntityIterator, error) {
	bounds, err := resolveUintBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	if bounds.empty {
		return readstore.NewReverseSliceIterator(nil), nil
	}

	if bounds.isEquality() {
		exists, pErr := pebbleTxExists(ctx.pebbleReader, ctx.ledgerName, bounds.min)
		if pErr != nil {
			return nil, fmt.Errorf("checking tx existence: %w", pErr)
		}

		if !exists {
			return readstore.NewReverseSliceIterator(nil), nil
		}

		txIDBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(txIDBytes, bounds.min)

		rev := readstore.NewReverseSliceIterator([][]byte{txIDBytes})

		return trackReverseIterator(rev, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("ReverseSliceIterator(pebble:%s:tx:id=%d)", ctx.ledgerName, bounds.min),
			Kind:   "Slice",
			Prefix: "pebble:txupdate",
		}), nil
	}

	var lower, upper []byte

	if bounds.hasMin {
		lower = make([]byte, 8)
		binary.BigEndian.PutUint64(lower, bounds.min)
	}

	if bounds.hasMax {
		upper = make([]byte, 8)
		binary.BigEndian.PutUint64(upper, bounds.max)
	}

	rangeIter, pErr := readstore.NewReversePebbleTxRangeIterator(ctx.pebbleReader, ctx.ledgerName, lower, upper)
	if pErr != nil {
		return nil, fmt.Errorf("creating reverse tx range iterator: %w", pErr)
	}

	return trackReverseIterator(rangeIter, ctx.profile, &IteratorStats{
		Label:  fmt.Sprintf("ReversePebbleTxRangeIterator(%s:id range)", ctx.ledgerName),
		Kind:   "PebbleTxRange",
		Prefix: "pebble:txupdate",
	}), nil
}

func reverseTimestampCondition(ctx *compileCtx, cond *commonpb.UintCondition) (ReverseEntityIterator, error) {
	if _, err := requireIndexReady(ctx,
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
		"timestamp"); err != nil {
		return nil, err
	}

	return reverseTimestampRangeCondition(ctx, cond,
		readstore.TransactionTimestampRangePrefix(ctx.kb, ctx.ledgerName), "tstmp", 0)
}

func reverseInsertedAtCondition(ctx *compileCtx, cond *commonpb.UintCondition) (ReverseEntityIterator, error) {
	if _, err := requireIndexReady(ctx,
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT),
		"inserted_at"); err != nil {
		return nil, err
	}

	return reverseTimestampRangeCondition(ctx, cond,
		readstore.TransactionInsertedAtRangePrefix(ctx.kb, ctx.ledgerName), "txiat", 0)
}

func reverseRevertedAtCondition(ctx *compileCtx, cond *commonpb.UintCondition) (ReverseEntityIterator, error) {
	if _, err := requireIndexReady(ctx,
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT),
		"reverted_at"); err != nil {
		return nil, err
	}

	return reverseTimestampRangeCondition(ctx, cond,
		readstore.TransactionRevertedAtRangePrefix(ctx.kb, ctx.ledgerName), "rvat", ctx.pin)
}

func reverseTimestampRangeCondition(
	ctx *compileCtx,
	cond *commonpb.UintCondition,
	ledgerPrefix []byte,
	bucketLabel string,
	stampPin uint64,
) (ReverseEntityIterator, error) {
	bounds, err := resolveUintBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	if bounds.empty {
		return readstore.NewReverseSliceIterator(nil), nil
	}

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

	iter, rErr := readstore.NewStampGatedRangeIterator(ctx.indexReader, lower, upper, entityOffset, entityLen, stampPin)
	if rErr != nil {
		return nil, fmt.Errorf("creating timestamp range iterator: %w", rErr)
	}

	stats := &IteratorStats{
		Label:  fmt.Sprintf("ReverseSliceIterator(%s:%s range)", bucketLabel, ctx.ledgerName),
		Kind:   "Range",
		Prefix: bucketLabel,
	}
	matIter, err := materializeIterator(iter, ctx.profile, stats)
	if err != nil {
		return nil, err
	}

	return trackReverseIterator(readstore.NewReverseSliceIterator(matIter.entities), ctx.profile, stats), nil
}

func reverseLogBuiltinUintCondition(ctx *compileCtx, cond *commonpb.LogBuiltinUintCondition) (ReverseEntityIterator, error) {
	if cond.GetCond() == nil {
		return nil, domain.NewFilterCompilationError("log builtin uint condition has no value")
	}

	switch cond.GetField() {
	case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE:
		return reverseLogDateCondition(ctx, cond.GetCond())
	default:
		return nil, domain.NewFilterCompilationError("unsupported log builtin uint field: %v", cond.GetField())
	}
}

func reverseLogDateCondition(ctx *compileCtx, cond *commonpb.UintCondition) (ReverseEntityIterator, error) {
	if _, err := requireIndexReady(ctx,
		indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
		"log date"); err != nil {
		return nil, err
	}

	return reverseTimestampRangeCondition(ctx, cond,
		readstore.LedgerLogDateRangePrefix(ctx.kb, ctx.ledgerName), "lldt", 0)
}

func reverseLogIdCondition(ctx *compileCtx, cond *commonpb.UintCondition) (ReverseEntityIterator, error) {
	if cond == nil {
		return reverseUniverse(ctx)
	}

	bounds, err := resolveUintBounds(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	if bounds.empty {
		return readstore.NewReverseSliceIterator(nil), nil
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
			return readstore.NewReverseSliceIterator(nil), nil
		}

		rev := readstore.NewReverseSliceIterator([][]byte{logIDBytes})

		return trackReverseIterator(rev, ctx.profile, &IteratorStats{
			Label:  fmt.Sprintf("ReverseSliceIterator(llog:%s:id=%d)", ctx.ledgerName, bounds.min),
			Kind:   "Slice",
			Prefix: "llog",
		}), nil
	}

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
		Label:  fmt.Sprintf("ReverseSliceIterator(llog:%s:id range)", ctx.ledgerName),
		Kind:   "Range",
		Prefix: "llog",
	}
	matIter, err := materializeIterator(iter, ctx.profile, stats)
	if err != nil {
		return nil, err
	}

	return trackReverseIterator(readstore.NewReverseSliceIterator(matIter.entities), ctx.profile, stats), nil
}

// trackReverseIterator wraps a reverse iterator with a TrackedReverseIterator
// when profiling is active, and sets profile.Root to the new stats node.
func trackReverseIterator(iter ReverseEntityIterator, profile *QueryProfile, stats *IteratorStats) ReverseEntityIterator {
	if profile == nil {
		return iter
	}

	profile.Root = stats

	return NewTrackedReverseIterator(iter, stats)
}

func closeAllReverse(iters []ReverseEntityIterator) {
	for _, it := range iters {
		it.Close()
	}
}
