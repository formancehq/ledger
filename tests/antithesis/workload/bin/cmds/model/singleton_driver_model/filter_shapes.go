package main

import (
	"math"

	"github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// The query generators build filters the model can predict a page for. The
// functions below cover the rest of the wire shape: any QueryFilter arm on any
// target, every oneof left unset or set to any member, optional fields absent
// or present, enums outside their declared values, empty strings, parameter
// references, inverted ranges and trees past the depth limit. Whether the
// server must refuse the result is decided from the filter itself
// (classifyRejectedFilter for a query, the oracle's write-time validation for a
// prepared-query save); what it serves when it accepts is judged like any
// other page.

// rollFilterShape rewrites one node of f into an arbitrary shape, one call in
// eight. A nil f is the root.
func rollFilterShape(f *commonpb.QueryFilter) *commonpb.QueryFilter {
	if !oneIn(8) {
		return f
	}

	return mutateFilter(f)
}

// mutateFilter returns a copy of f with one node, picked uniformly in tree
// order, rewritten.
func mutateFilter(f *commonpb.QueryFilter) *commonpb.QueryFilter {
	if f == nil {
		return genAnyFilter(0)
	}

	out := f.CloneVT()

	var nodes []*commonpb.QueryFilter
	collectNodes(out, &nodes)
	node := nodes[internal.Rand().Intn(len(nodes))]

	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4}) {
	case 0:
		node.Filter = nil
	case 1:
		node.Filter = &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: &commonpb.QueryFilter{Filter: node.GetFilter()}}}
	case 2:
		node.Filter = genAnyLeaf().GetFilter()
	case 3:
		node.Filter = genAnyFilter(1).GetFilter()
	default:
		node.Filter = depthChain(&commonpb.QueryFilter{Filter: node.GetFilter()}).GetFilter()
	}

	return out
}

// collectNodes lists every node of f in tree order. Repeated children are
// never nil: a nil element marshals as an empty node, which genAnyFilter
// produces explicitly when it wants one.
func collectNodes(f *commonpb.QueryFilter, out *[]*commonpb.QueryFilter) {
	*out = append(*out, f)

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, child := range x.And.GetFilters() {
			collectNodes(child, out)
		}
	case *commonpb.QueryFilter_Or:
		for _, child := range x.Or.GetFilters() {
			collectNodes(child, out)
		}
	case *commonpb.QueryFilter_Not:
		if child := x.Not.GetFilter(); child != nil {
			collectNodes(child, out)
		}
	}
}

// depthChain wraps f in Not nodes so the tree lands just under, at or just past
// MaxFilterDepth.
func depthChain(f *commonpb.QueryFilter) *commonpb.QueryFilter {
	n := domain.MaxFilterDepth - 2 + internal.Rand().Intn(4)
	for range n {
		f = &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: f}}}
	}

	return f
}

// genAnyFilter rolls an arbitrary tree: combinators over zero to three
// children, an empty node, or any leaf.
func genAnyFilter(depth int) *commonpb.QueryFilter {
	if depth >= maxQueryGenDepth || oneIn(2) {
		if oneIn(8) {
			return &commonpb.QueryFilter{}
		}

		return genAnyLeaf()
	}

	children := func() []*commonpb.QueryFilter {
		out := make([]*commonpb.QueryFilter, internal.Rand().Intn(4))
		for i := range out {
			out[i] = genAnyFilter(depth + 1)
		}

		return out
	}

	switch random.RandomChoice([]uint8{0, 1, 2, 3}) {
	case 0:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: children()}}}
	case 1:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: children()}}}
	case 2:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{}}}
	default:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: genAnyFilter(depth + 1)}}}
	}
}

// genAnyLeaf rolls one leaf of any arm, with every sub-field drawn
// independently.
func genAnyLeaf() *commonpb.QueryFilter {
	switch internal.Rand().Intn(11) {
	case 0:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: anyFieldCondition()}}
	case 1:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: anyAddressMatch()}}
	case 2:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reference{Reference: &commonpb.ReferenceCondition{Cond: anyStringCondition()}}}
	case 3:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{BuiltinUint: &commonpb.BuiltinUintCondition{
			Field: commonpb.TransactionBuiltinIndex(anyEnum(7)),
			Cond:  anyUintCondition(),
		}}}
	case 4:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Ledger{Ledger: &commonpb.LedgerCondition{Cond: anyStringCondition()}}}
	case 5:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogId{LogId: &commonpb.LogIdCondition{Cond: anyUintCondition()}}}
	case 6:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogBuiltinUint{LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
			Field: commonpb.LogBuiltinIndex(anyEnum(1)),
			Cond:  anyUintCondition(),
		}}}
	case 7:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_AccountHasAsset{AccountHasAsset: &commonpb.AccountHasAssetCondition{
			AssetBase: random.RandomChoice([]string{"", "USD", "EUR", "COIN", "ZZZ"}),
			Precision: random.RandomChoice([]uint32{0, 2, math.MaxUint8, math.MaxUint8 + 1, math.MaxUint32}),
		}}}
	case 8:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reverted{Reverted: &commonpb.RevertedCondition{Value: oneIn(2)}}}
	case 9:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: anyAuditCondition()}}
	default:
		return &commonpb.QueryFilter{}
	}
}

// anyEnum draws an enum value: one of 0..maxDeclared, or one past it, or far
// past it.
func anyEnum(maxDeclared int32) int32 {
	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5, 6, 7}) {
	case 0:
		return maxDeclared + 1
	case 1:
		return math.MaxInt32
	default:
		return int32(internal.Rand().Intn(int(maxDeclared) + 1))
	}
}

func anyString() string {
	return random.RandomChoice([]string{"", "a", "\x00", poolName() + ":", poolAddress(), metaKey()})
}

func anyParamName() string {
	return random.RandomChoice([]string{"", "p0", "p1"})
}

func anyStringCondition() *commonpb.StringCondition {
	switch random.RandomChoice([]uint8{0, 1, 2, 3}) {
	case 0:
		return nil
	case 1:
		return &commonpb.StringCondition{}
	case 2:
		return &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: anyString()}}
	default:
		return &commonpb.StringCondition{Value: &commonpb.StringCondition_Param{Param: anyParamName()}}
	}
}

func anyBoolCondition() *commonpb.BoolCondition {
	switch random.RandomChoice([]uint8{0, 1, 2}) {
	case 0:
		return &commonpb.BoolCondition{}
	case 1:
		return &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: oneIn(2)}}
	default:
		return &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Param{Param: anyParamName()}}
	}
}

func anyUintCondition() *commonpb.UintCondition {
	if oneIn(6) {
		return nil
	}

	bound := func() *uint64 {
		if oneIn(2) {
			return nil
		}
		v := random.RandomChoice([]uint64{0, 1, 2, 5, 100, math.MaxUint64 - 1, math.MaxUint64, internal.Rand().Uint64()})

		return &v
	}
	cond := &commonpb.UintCondition{Min: bound(), Max: bound(), MinExclusive: oneIn(2), MaxExclusive: oneIn(2)}
	if oneIn(6) {
		cond.ParamMin = anyParamName()
	}
	if oneIn(6) {
		cond.ParamMax = anyParamName()
	}

	return cond
}

func anyIntCondition() *commonpb.IntCondition {
	bound := func() *int64 {
		if oneIn(2) {
			return nil
		}
		v := random.RandomChoice([]int64{math.MinInt64, -1, 0, 1, 5, math.MaxInt64})

		return &v
	}
	cond := &commonpb.IntCondition{Min: bound(), Max: bound(), MinExclusive: oneIn(2), MaxExclusive: oneIn(2)}
	if oneIn(6) {
		cond.ParamMin = anyParamName()
	}
	if oneIn(6) {
		cond.ParamMax = anyParamName()
	}

	return cond
}

func anyFieldCondition() *commonpb.FieldCondition {
	fc := &commonpb.FieldCondition{}
	if !oneIn(6) {
		fc.Field = &commonpb.FieldRef{Metadata: random.RandomChoice([]string{"", metaKey(), metaKey()})}
	}

	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5}) {
	case 0:
	case 1:
		fc.Condition = &commonpb.FieldCondition_StringCond{StringCond: anyStringConditionNonNil()}
	case 2:
		fc.Condition = &commonpb.FieldCondition_IntCond{IntCond: anyIntCondition()}
	case 3:
		fc.Condition = &commonpb.FieldCondition_UintCond{UintCond: anyUintConditionNonNil()}
	case 4:
		fc.Condition = &commonpb.FieldCondition_BoolCond{BoolCond: anyBoolCondition()}
	default:
		fc.Condition = &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{IncludeNull: oneIn(2)}}
	}

	return fc
}

// anyStringConditionNonNil and anyUintConditionNonNil fill a oneof member, which
// cannot hold a nil message on the wire.
func anyStringConditionNonNil() *commonpb.StringCondition {
	if c := anyStringCondition(); c != nil {
		return c
	}

	return &commonpb.StringCondition{}
}

func anyUintConditionNonNil() *commonpb.UintCondition {
	if c := anyUintCondition(); c != nil {
		return c
	}

	return &commonpb.UintCondition{}
}

func anyAddressMatch() *commonpb.AddressMatch {
	am := &commonpb.AddressMatch{Role: commonpb.AddressRole(anyEnum(2))}

	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4}) {
	case 0:
	case 1:
		am.Match = &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: anyString()}
	case 2:
		am.Match = &commonpb.AddressMatch_HardcodedExact{HardcodedExact: anyString()}
	case 3:
		am.Match = &commonpb.AddressMatch_ParamPrefix{ParamPrefix: anyParamName()}
	default:
		am.Match = &commonpb.AddressMatch_ParamExact{ParamExact: anyParamName()}
	}

	return am
}

func anyAuditCondition() *commonpb.AuditCondition {
	ac := &commonpb.AuditCondition{Field: commonpb.AuditField(anyEnum(9))}

	switch random.RandomChoice([]uint8{0, 1, 2, 3}) {
	case 0:
	case 1:
		ac.Condition = &commonpb.AuditCondition_StringCond{StringCond: anyStringConditionNonNil()}
	case 2:
		ac.Condition = &commonpb.AuditCondition_UintCond{UintCond: anyUintConditionNonNil()}
	default:
		ac.Condition = &commonpb.AuditCondition_StringPrefix{StringPrefix: anyString()}
	}

	return ac
}
