package main

import (
	"math"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
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
func rollFilterShape(src *generation.Source, f *commonpb.QueryFilter) *commonpb.QueryFilter {
	if !oneIn(src, 8) {
		return f
	}

	return mutateFilter(src, f)
}

// mutateFilter returns a copy of f with one node, picked uniformly in tree
// order, rewritten.
func mutateFilter(src *generation.Source, f *commonpb.QueryFilter) *commonpb.QueryFilter {
	if f == nil {
		return genAnyFilter(src, 0)
	}

	out := f.CloneVT()

	var nodes []*commonpb.QueryFilter
	collectNodes(out, &nodes)
	node := nodes[src.Intn(len(nodes))]

	switch generation.Choice(src, []uint8{0, 1, 2, 3, 4}) {
	case 0:
		node.Filter = nil
	case 1:
		node.Filter = &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: &commonpb.QueryFilter{Filter: node.GetFilter()}}}
	case 2:
		node.Filter = genAnyLeaf(src).GetFilter()
	case 3:
		node.Filter = genAnyFilter(src, 1).GetFilter()
	default:
		node.Filter = depthChain(src, &commonpb.QueryFilter{Filter: node.GetFilter()}).GetFilter()
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
func depthChain(src *generation.Source, f *commonpb.QueryFilter) *commonpb.QueryFilter {
	n := domain.MaxFilterDepth - 2 + src.Intn(4)
	for range n {
		f = &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: f}}}
	}

	return f
}

// genAnyFilter rolls an arbitrary tree: combinators over zero to three
// children, an empty node, or any leaf.
func genAnyFilter(src *generation.Source, depth int) *commonpb.QueryFilter {
	if depth >= maxQueryGenDepth || oneIn(src, 2) {
		if oneIn(src, 8) {
			return &commonpb.QueryFilter{}
		}

		return genAnyLeaf(src)
	}

	children := func() []*commonpb.QueryFilter {
		out := make([]*commonpb.QueryFilter, src.Intn(4))
		for i := range out {
			out[i] = genAnyFilter(src, depth+1)
		}

		return out
	}

	switch generation.Choice(src, []uint8{0, 1, 2, 3}) {
	case 0:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: children()}}}
	case 1:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: children()}}}
	case 2:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{}}}
	default:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: genAnyFilter(src, depth+1)}}}
	}
}

// genAnyLeaf rolls one leaf of any arm, with every sub-field drawn
// independently.
func genAnyLeaf(src *generation.Source) *commonpb.QueryFilter {
	switch src.Intn(11) {
	case 0:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: anyFieldCondition(src)}}
	case 1:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: anyAddressMatch(src)}}
	case 2:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reference{Reference: &commonpb.ReferenceCondition{Cond: anyStringCondition(src)}}}
	case 3:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{BuiltinUint: &commonpb.BuiltinUintCondition{
			Field: commonpb.TransactionBuiltinIndex(anyEnum(src, 7)),
			Cond:  anyUintCondition(src),
		}}}
	case 4:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Ledger{Ledger: &commonpb.LedgerCondition{Cond: anyStringCondition(src)}}}
	case 5:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogId{LogId: &commonpb.LogIdCondition{Cond: anyUintCondition(src)}}}
	case 6:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogBuiltinUint{LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
			Field: commonpb.LogBuiltinIndex(anyEnum(src, 1)),
			Cond:  anyUintCondition(src),
		}}}
	case 7:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_AccountHasAsset{AccountHasAsset: &commonpb.AccountHasAssetCondition{
			AssetBase: generation.Choice(src, []string{"", "USD", "EUR", "COIN", "ZZZ"}),
			Precision: generation.Choice(src, []uint32{0, 2, math.MaxUint8, math.MaxUint8 + 1, math.MaxUint32}),
		}}}
	case 8:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reverted{Reverted: &commonpb.RevertedCondition{Value: oneIn(src, 2)}}}
	case 9:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: anyAuditCondition(src)}}
	default:
		return &commonpb.QueryFilter{}
	}
}

// anyEnum draws an enum value: one of 0..maxDeclared, or one past it, or far
// past it.
func anyEnum(src *generation.Source, maxDeclared int32) int32 {
	switch generation.Choice(src, []uint8{0, 1, 2, 3, 4, 5, 6, 7}) {
	case 0:
		return maxDeclared + 1
	case 1:
		return math.MaxInt32
	default:
		return int32(src.Intn(int(maxDeclared) + 1))
	}
}

func anyString(src *generation.Source) string {
	return generation.Choice(src, []string{"", "a", "\x00", poolName(src) + ":", poolAddress(src), metaKey(src)})
}

func anyParamName(src *generation.Source) string {
	return generation.Choice(src, []string{"", "p0", "p1"})
}

func anyStringCondition(src *generation.Source) *commonpb.StringCondition {
	switch generation.Choice(src, []uint8{0, 1, 2, 3}) {
	case 0:
		return nil
	case 1:
		return &commonpb.StringCondition{}
	case 2:
		return &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: anyString(src)}}
	default:
		return &commonpb.StringCondition{Value: &commonpb.StringCondition_Param{Param: anyParamName(src)}}
	}
}

func anyBoolCondition(src *generation.Source) *commonpb.BoolCondition {
	switch generation.Choice(src, []uint8{0, 1, 2}) {
	case 0:
		return &commonpb.BoolCondition{}
	case 1:
		return &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: oneIn(src, 2)}}
	default:
		return &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Param{Param: anyParamName(src)}}
	}
}

func anyUintCondition(src *generation.Source) *commonpb.UintCondition {
	if oneIn(src, 6) {
		return nil
	}

	bound := func() *uint64 {
		if oneIn(src, 2) {
			return nil
		}
		v := generation.Choice(src, []uint64{0, 1, 2, 5, 100, math.MaxUint64 - 1, math.MaxUint64, src.Uint64()})

		return &v
	}
	cond := &commonpb.UintCondition{Min: bound(), Max: bound(), MinExclusive: oneIn(src, 2), MaxExclusive: oneIn(src, 2)}
	if oneIn(src, 6) {
		cond.ParamMin = anyParamName(src)
	}
	if oneIn(src, 6) {
		cond.ParamMax = anyParamName(src)
	}

	return cond
}

func anyIntCondition(src *generation.Source) *commonpb.IntCondition {
	bound := func() *int64 {
		if oneIn(src, 2) {
			return nil
		}
		v := generation.Choice(src, []int64{math.MinInt64, -1, 0, 1, 5, math.MaxInt64})

		return &v
	}
	cond := &commonpb.IntCondition{Min: bound(), Max: bound(), MinExclusive: oneIn(src, 2), MaxExclusive: oneIn(src, 2)}
	if oneIn(src, 6) {
		cond.ParamMin = anyParamName(src)
	}
	if oneIn(src, 6) {
		cond.ParamMax = anyParamName(src)
	}

	return cond
}

func anyFieldCondition(src *generation.Source) *commonpb.FieldCondition {
	fc := &commonpb.FieldCondition{}
	if !oneIn(src, 6) {
		fc.Field = &commonpb.FieldRef{Metadata: generation.Choice(src, []string{"", metaKey(src), metaKey(src)})}
	}

	switch generation.Choice(src, []uint8{0, 1, 2, 3, 4, 5}) {
	case 0:
	case 1:
		fc.Condition = &commonpb.FieldCondition_StringCond{StringCond: anyStringConditionNonNil(src)}
	case 2:
		fc.Condition = &commonpb.FieldCondition_IntCond{IntCond: anyIntCondition(src)}
	case 3:
		fc.Condition = &commonpb.FieldCondition_UintCond{UintCond: anyUintConditionNonNil(src)}
	case 4:
		fc.Condition = &commonpb.FieldCondition_BoolCond{BoolCond: anyBoolCondition(src)}
	default:
		fc.Condition = &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{IncludeNull: oneIn(src, 2)}}
	}

	return fc
}

// anyStringConditionNonNil and anyUintConditionNonNil fill a oneof member, which
// cannot hold a nil message on the wire.
func anyStringConditionNonNil(src *generation.Source) *commonpb.StringCondition {
	if c := anyStringCondition(src); c != nil {
		return c
	}

	return &commonpb.StringCondition{}
}

func anyUintConditionNonNil(src *generation.Source) *commonpb.UintCondition {
	if c := anyUintCondition(src); c != nil {
		return c
	}

	return &commonpb.UintCondition{}
}

func anyAddressMatch(src *generation.Source) *commonpb.AddressMatch {
	am := &commonpb.AddressMatch{Role: commonpb.AddressRole(anyEnum(src, 2))}

	switch generation.Choice(src, []uint8{0, 1, 2, 3, 4}) {
	case 0:
	case 1:
		am.Match = &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: anyString(src)}
	case 2:
		am.Match = &commonpb.AddressMatch_HardcodedExact{HardcodedExact: anyString(src)}
	case 3:
		am.Match = &commonpb.AddressMatch_ParamPrefix{ParamPrefix: anyParamName(src)}
	default:
		am.Match = &commonpb.AddressMatch_ParamExact{ParamExact: anyParamName(src)}
	}

	return am
}

func anyAuditCondition(src *generation.Source) *commonpb.AuditCondition {
	ac := &commonpb.AuditCondition{Field: commonpb.AuditField(anyEnum(src, 9))}

	switch generation.Choice(src, []uint8{0, 1, 2, 3}) {
	case 0:
	case 1:
		ac.Condition = &commonpb.AuditCondition_StringCond{StringCond: anyStringConditionNonNil(src)}
	case 2:
		ac.Condition = &commonpb.AuditCondition_UintCond{UintCond: anyUintConditionNonNil(src)}
	default:
		ac.Condition = &commonpb.AuditCondition_StringPrefix{StringPrefix: anyString(src)}
	}

	return ac
}
